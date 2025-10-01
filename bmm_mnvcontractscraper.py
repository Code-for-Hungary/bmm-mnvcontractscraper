import requests
import datetime
import configparser
import huspacy
import re
import logging
from jinja2 import Environment, FileSystemLoader, select_autoescape
from bmmbackend import bmmbackend
import bmmtools
from bmm_mnvdb import Bmm_MNVDB
from difflib import SequenceMatcher

def download_data():

    url = config['Download']['url']
    
    d = datetime.datetime.strptime(config['Download']['from_date'], '%Y-%m-%d')
    from_date = bmmtools.mnvtimestamp(d.timestamp())

    most = datetime.datetime.now();

    to_date = bmmtools.mnvtimestamp(most.timestamp())

    start = 0
    size = int(config['Download']['size'])

    while True:

        params = {
            'fromDate': from_date,
            'toDate': to_date,
            'from': start,
            'size': size
        }

        try:
            response = requests.get(url, params=params, verify=False)
            response = response.json()
        except requests.RequestException as e:
            logging.error(f"Network error during data download: {e}")
            break
        except ValueError as e:
            logging.error(f"JSON parsing error during data download: {e}")
            break

        if not 'errors' in response:
            data = response['hits']
            for entry in data:
                number = entry['number']

                try:
                    if db.getContract(number) is None:
                        d = datetime.datetime.fromtimestamp(entry['date'] / 1000).strftime('%Y-%m-%d')

                        lemmas = []
                        doc = nlp(entry['subject'])
                        for t in doc:
                            if t.pos_ in ['NOUN', 'ADJ', 'PROPN', 'ADP', 'ADV', 'VERB'] and t.lemma_.isalpha():
                                lemmas.append(t.lemma_.lower())

                        lemmas = " ".join(lemmas)

                        db.saveContract(number, d, entry, lemmas)
                    else:
                        continue
                except Exception as e:
                    logging.error(f"Database error while processing contract {number}: {e}")
                    continue

            try:
                db.commitConnection()
            except Exception as e:
                logging.error(f"Database commit error: {e}")
        else:
            logging.error(f"API returned errors: {response['errors']}")
            print('Hiba az adatok letöltése közben')
            break

        if len(data) < size:
            break

        start += size

    try:
        config.set('Download', 'from_date', most.strftime('%Y-%m-%d'))
        with open('config.ini', 'w') as configfile:
            config.write(configfile)
    except Exception as e:
        logging.error(f"Error updating config file: {e}")

def clearIsNew(ids):
    
    try:
        for num in ids:
            db.clearIsNew(num)

        db.commitConnection()
    except Exception as e:
        logging.error(f"Error clearing isNew flags: {e}")


def search(text, keyword, nlp_warn=False):
    keyword = keyword.replace('*', '').replace('"', '')
    results = []
    matches = [m.start() for m in re.finditer(re.escape(keyword), text, re.IGNORECASE)]
    words = text.split()

    for match_index in matches:
        # Convert character index to word index
        char_count = 0
        word_index = 0

        for word_index, word in enumerate(words):
            char_count += len(word) + 1  # +1 accounts for spaces
            if char_count > match_index:
                break

        # Get surrounding 10 words before and after the match
        before = " ".join(words[max(word_index - 16, 0) : word_index])
        after = " ".join(words[word_index + 1 : word_index + 17])
        found_word = words[word_index]
        match = SequenceMatcher(
            None, found_word, event["parameters"]
        ).find_longest_match()
        match_before = found_word[: match.a]
        if match_before != "":
            before = before + " " + match_before
        else:
            before = before + " "
        match_after = found_word[match.a + match.size :]
        if match_after != "":
            after = match_after + " " + after
        else:
            after = " " + after
        common_part = found_word[match.a : match.a + match.size]

        if nlp_warn:
            before = "szótövezett találat: " + before

        results.append(
            {
                "before": before,
                "after": after,
                "common": common_part,
            }
        )
    return results

def find_matching_multiple(keywords, entry, config):
    all_results = []
    print("Searching for keywords:", keywords)
    for keyword in keywords:
        keyword_results = search(entry["subject"], keyword)
        do_lemmatize = config['DEFAULT'].get('donotlemmatize', '0') == '0'
        if not keyword_results and do_lemmatize:
            keyword_results = search(entry["lemmasubject"], keyword, nlp_warn=True)
        all_results += keyword_results
    return all_results


config = configparser.ConfigParser()
config.read_file(open('config.ini'))
api_key = config['DEFAULT']['eventgenerator_api_key']

logging.basicConfig(
    filename=config['DEFAULT']['logfile_name'], 
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s | %(module)s.%(funcName)s line %(lineno)d: %(message)s')

logging.info('MNVContractScraper started')

db = Bmm_MNVDB(config['DEFAULT']['database_name'])
backend = bmmbackend(config['DEFAULT']['monitor_url'], config['DEFAULT']['uuid'])

foundIds = []

env = Environment(
    loader=FileSystemLoader('templates'),
    autoescape=select_autoescape()
)
contenttpl = env.get_template('content.html')
contenttpl_keyword = env.get_template('content_keyword.html')

nlp = huspacy.load()
download_data()

try:
    events = backend.getEvents(api_key)
except Exception as e:
    logging.error(f"Error fetching events from backend: {e}")
    events = {'data': []}

new_entries = db.getAllNew()
print(new_entries)

for event in events['data']:
    result = None
    content = ''

    try:
        if event['type'] == 1:  # Event is of specific keyword
            for entry in new_entries:
                search_results = find_matching_multiple(event['parameters'].split(","), entry, config)
                result_entry = entry.copy()
                result_entry["result_count"] = len(search_results)
                result_entry["results"] = search_results[:5]
                if result_entry["results"]:
                    content += contenttpl_keyword.render(contract = result_entry)
                foundIds.append(entry["number"])

        else:
            for entry in new_entries:
                content = content + contenttpl.render(contract = entry)
                foundIds.append(entry["number"])

        if config['DEFAULT']['donotnotify'] == '0' and content:
            try:
                backend.notifyEvent(event['id'], content, api_key)
                logging.info(f"Notified: {event['id']} - {event['type']} - {event['parameters']}")
            except Exception as e:
                logging.error(f"Error notifying event {event['id']}: {e}")

    except Exception as e:
        logging.error(f"Error processing event {event.get('id', 'unknown')}: {e}")

if config['DEFAULT']['staging'] == '0':
    clearIsNew(foundIds)

try:
    db.closeConnection()
except Exception as e:
    logging.error(f"Error closing database connection: {e}")

logging.info('MNVContractScraper ready. Bye.')

print('Ready. Bye.')

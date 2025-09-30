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


def search(entry, keyword, do_lemmatize=False):
    text = entry["subject"] if not do_lemmatize else entry["lemmasubject"]
    keyword = keyword.replace('*', '').replace('"', '')
    results = []
    matches = [m.start() for m in re.finditer(re.escape(keyword), text, re.IGNORECASE)]

    surrounding_context = 64

    for match_index in matches:
        before_context = text[max(0, match_index-surrounding_context):match_index]
        after_context = text[match_index+len(keyword):match_index+len(keyword)+surrounding_context]
        common_part = text[match_index:match_index+len(keyword)]

        lemma_warn = ''
        if do_lemmatize:
            lemma_warn = "szótövezett találat: "

        results.append(
            {
                "before": lemma_warn+before_context,
                "after": after_context,
                "common": common_part,
            }
        )
    return results

def find_matching_multiple(keywords, entry, config):
    all_results = []
    print("Searching for keywords:", keywords)
    for keyword in keywords:
        keyword_results = search(entry, keyword)
        if not keyword_results and config['DEFAULT'].get('donotlemmatize', '0') == '0':
            keyword_results = search(entry, keyword, do_lemmatize=True)
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

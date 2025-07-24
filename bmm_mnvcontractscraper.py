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

nlp = huspacy.load()
download_data()

try:
    events = backend.getEvents(api_key)
except Exception as e:
    logging.error(f"Error fetching events from backend: {e}")
    events = {'data': []}

for event in events['data']:
    result = None

    try:
        if event['type'] == 1:
            keresoszo = bmmtools.searchstringtofts(event['parameters'])
            if keresoszo:
                result = db.searchRecords(keresoszo)
                for res in result:
                    foundIds.append(res[0])
        else:
            result = db.getAllNew()
            for res in result:
                foundIds.append(res[0])
    except Exception as e:
        logging.error(f"Error processing event {event.get('id', 'unknown')}: {e}")
        continue

    if result:
        content = ''
        for res in result:
            content = content + contenttpl.render(contract = res)

        if config['DEFAULT']['donotnotify'] == '0':
            try:
                backend.notifyEvent(event['id'], content, api_key)
                logging.info(f"Notified: {event['id']} - {event['type']} - {event['parameters']}")
            except Exception as e:
                logging.error(f"Error notifying event {event['id']}: {e}")

if config['DEFAULT']['staging'] == '0':
    clearIsNew(foundIds)

try:
    db.closeConnection()
except Exception as e:
    logging.error(f"Error closing database connection: {e}")

logging.info('MNVContractScraper ready. Bye.')

print('Ready. Bye.')

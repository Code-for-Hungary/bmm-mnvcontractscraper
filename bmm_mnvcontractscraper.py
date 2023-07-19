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

        response = requests.get(url, params=params)
        response = response.json()
        if not 'errors' in response:
            data = response['hits']
            for entry in data:
                number = entry['number']

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

            db.commitConnection()
        else:
            print('Hiba az adatok letöltése közben')
            break

        if len(data) < size:
            break

        start += size

    config.set('Download', 'from_date', most.strftime('%Y-%m-%d'))
    with open('config.ini', 'w') as configfile:
        config.write(configfile)

def clearIsNew(ids):
    
    for num in ids:
        db.clearIsNew(num)

    db.commitConnection()


config = configparser.ConfigParser()
config.read_file(open('config.ini'))

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

events = backend.getEvents()
for event in events['data']:
    result = None

    keresoszo = bmmtools.searchstringtofts(event['parameters'])
    if keresoszo:
        result = db.searchRecords(keresoszo)
        for res in result:
            foundIds.append(res[0])

    if result:
        content = ''
        for res in result:
            content = content + contenttpl.render(contract = res)
            
        backend.notifyEvent(event['id'], content)

if config['DEFAULT']['staging'] == '0':
    clearIsNew(foundIds)

db.closeConnection()

logging.info('MNVContractScraper ready. Bye.')

print('Ready. Bye.')

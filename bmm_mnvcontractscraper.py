import requests
import datetime
import configparser
import huspacy
import re
from jinja2 import Environment, FileSystemLoader, select_autoescape
from bmm import Bmm
from bmm_mnvdb import Bmm_MNVDB

def download_data():

    url = config['Download']['url']
    
    d = datetime.datetime.strptime(config['Download']['from_date'], '%Y-%m-%d')
    from_date = int(d.timestamp()) * 1000

    most = datetime.datetime.now();

    to_date = int(most.timestamp()) * 1000

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
                        if t.pos_ in ['NOUN', 'ADJ', 'PROPN', 'ADP', 'ADV', 'VERB']:
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

db = Bmm_MNVDB(config['DEFAULT']['database_name'])
backend = Bmm(config['DEFAULT']['monitor_url'], config['DEFAULT']['uuid'])

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

    keresoszo = event['parameters'].strip()
    keresoszo = re.sub(r'\s+', ' ', keresoszo)
    keresoszo = re.sub(r'([()])', '', keresoszo)
    if keresoszo:
        if not re.search(r'(["+\-~*])', keresoszo):
            keresoszo = re.sub(r'([\s])', ' +', keresoszo) + '*'
        
        print(keresoszo)

        result = db.searchRecords(keresoszo)
        for res in result:
            foundIds.append(res[0])

    if result:
        content = ''
        for res in result:
            print(res[2])
            print(type(res[2]))
            content = content + contenttpl.render(contract = res)
            
        backend.notifyEvent(event['id'], content)

if not config['DEFAULT']['staging']:
    clearIsNew(foundIds)

db.closeConnection()

print('Ready. Bye.')

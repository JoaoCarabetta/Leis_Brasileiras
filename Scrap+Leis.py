from pymongo import MongoClient
import time
import pandas as pd
import urllib
import xml.etree.ElementTree as ET
from xmljson import parker as pk
from xml.etree.ElementTree import fromstring
from json import dumps, loads
import pprint
import html2text
import copy
from tqdm import tqdm
import sys
from pprint import pprint

def connect_database():
    client = MongoClient()
    client = MongoClient('localhost', 27017)
    db = client['leis']
    return db['federais']

def get_content(url):

    html = urllib.request.urlopen(url)
    tree = ET.parse(html)
    root = tree.getroot()

    return root

def to_database(root):
    try:
        leis = loads(dumps(pk.data(root)))['Normas']['Norma']
        result = collection.insert_many(leis)
    except:
        print('Vazio')


def parse_html(url, id):

    html = urllib.request.urlopen(url)
    
    html = str(html.read().decode('utf-8'))
    
    with open('htmls/{}.html'.format(id), 'w') as f:
        f.write(html)

    text = html2text.html2text(html)
    
    return text    

def update_db(root, id):
    # add to db with LinkTexto and processed html
    link = ""
    for l in root.iter('LinkTexto'):
        link = l.text

    # copy doc
    old_doc = collection.find_one({'Codigo': id})
    new_doc = copy.deepcopy(old_doc)
    
    # add Link Texto
    new_doc['LinkTexto'] = link
    
    # add html as text
    new_doc['TextContent'] = parse_html(link, id)
    
    # replace doc
    collection.find_one_and_replace({'Codigo': id}, new_doc, upsert=False)

def get_all_leis():
    print("Get Leis")
    for ano in tqdm(range(2016,2017)):
        root = get_content('http://legis.senado.leg.br/dadosabertos/legislacao/lista?tipo=LEI&ano={}'.format(ano))
        to_database(root)


def get_all_htmls(collection):

    for ano in range(1953, 1945, -1):
        print("\n\n ANO:", ano)

        for post in collection.find({'Ano': ano}):
            codigo = post['Codigo']
            
            for i in collection.find({'Codigo': codigo}):
                try:
                    i['TextContent']

                except KeyError:
                    print(codigo)
                    root = get_content("http://legis.senado.leg.br/dadosabertos/legislacao/{}".format(codigo))
                    update_db(root, codigo) 
                    
                    """except:
                                                                                    e = sys.exc_info()[0]
                                                                                    print(e)
                                                                                    with open("error.txt", 'a+') as f:
                                                                                        doc = f.read()
                                                                                        if str(codigo) not in doc:
                                                                                            f.write("{}: {},".format(codigo, e))
                                                            
 
                                                           """
def check_completude(collection):

    for ano in range(1945, 2017, 1):
        total = collection.find({"Ano": ano}).count()
        done = collection.find({ "$and" : [
                                                {'Ano': ano}, 
                                                {'TextContent': {"$exists": True}}]}).count()

        print("Ano: ", ano, 
            "| Numero de Leis: ", total,
            "| Completo: ", done,
            "| Faltam: ", total-done)


if __name__ == '__main__':
    collection = connect_database()
    # get_all_leis()
    #get_all_htmls(collection)
    check_completude(collection)

    # collection.drop()







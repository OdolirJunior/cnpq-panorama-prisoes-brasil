import unicodedata
import csv
import json
from collections import OrderedDict


def remover_acentos(text):
    text = unicodedata.normalize('NFD', text)
    text = text.encode('ascii', 'ignore')
    text = text.decode("utf-8")
    text = text.replace(' ', '-').lower()
    text = text.replace('\'', '')
    return text


def split_item(row):
    return [i.split(',', 1)[0] for i in row]

def format_title(title):
    x = remover_acentos(title)
    x = x.replace('.', '-')
    x = x.replace(',', '-')
    return x

def format_body(body):
    json_formated = {}
    for y in body:
        x = remover_acentos(y)
        json_formated.update({x: ''})
    return json_formated

class Spider(object):
    def scraping(self):
        body=[]
        title=[]
        with_key={}
        files = [
            'repository/a2017/estimativa_populacao_TCU_2017_20181108.csv',
            'repository/a2018/estimativa_populacao_TCU_2018_20190213.csv',
            'repository/a2017/DATA_SUS_Casos de aids identificados_2017.csv',
            'repository/a2016/estimativa_TCU_2016_20170614.csv',
            'repository/a2016/2016_basefinal_depen_publicacao-2016.csv',
            'repository/a2015/estimativa_populacao_TCU_2015_20170614.csv',
            'repository/a2015/2015_basefinal_depen_publicacao.csv',
            'repository/a2014/estimativa_populacao_TCU_2014_20170614.csv',
            'repository/a2014/DATA_SUS_Casos de aids identificados_2014.csv',
            'repository/a2014/csv-base-de-dados-infopen-2014.csv',
            'repository/a2011/DATA_SUS_atendimentos_por_violencias_2011',
            'repository/a2010/DATA_SUS_PROPORÇÃO DE PESSOAS COM BAIXA RENDA_2010.csv',
            'repository/a2010/DATA_SUS_Casos de aids identificados_2010.csv',
            'repository/a2000/DATA_SUS_PROPORÇÃO DE PESSOAS COM BAIXA RENDA_2000.csv'
            ]
        for file in files:
            with open(file,  'r', encoding='utf8', errors='ignore') as csvfile:
                readCSV = csv.reader(csvfile, delimiter=';')
                line_count = 0
                json_formated = {}
                result=[]
                for row in readCSV:
                    if line_count == 0:
                        x = [i.split(',', 1)[0] for i in row]
                        for y in row:
                            x = remover_acentos(y)
                            k = format_title(x)
                            json_formated.update({k: ''})
                    else:
                        y = [remover_acentos(i).split(',', 1)[0] for i in row]
                        dictionary = OrderedDict((k, v) for k, v in zip(json_formated, y) if v is not "")
                        dic_new = dictionary.copy()
                        for key, value in dic_new.items():
                            if value == 0 or value == '0':
                                del dictionary[key]
                            if value == 'no' or value == 'nao':
                                dictionary[key] = False
                            if value == 'sim' or value == 'si':
                                dictionary[key] = True
                        result.append(dictionary)
                        print(dictionary)
                    line_count += 1
                with open('JSONs/%s.json'% (file.split("/")[2]), 'w') as fp:
                    json.dump(result, fp, ensure_ascii=False)
                print('Arquivos JSONs gerados')






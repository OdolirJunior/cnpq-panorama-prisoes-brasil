import json
from hashlib import new
from dns import update, query
from pymongo import MongoClient
import pandas as pd

from spider import cities_state


def find_filter(list_inserted, param):
    fil = list(filter(lambda s: s == param, list_inserted))
    if len(fil) > 0:
        return False
    else:
        return True


class Process(object):
    def insert_data_mongo(self):
        client = MongoClient(
            "mongodb+srv://root:rq0wKDIdiMryckPn@cluster0-zjn1g.mongodb.net/test?retryWrites=true&w=majority")
        db = client["public"]
        collection = db["infopen_datasus_ibge_2014"]

        jsons_data_infopen = 'JSONs/csv-base-de-dados-infopen-2014.csv.json'
        # 'JSONs/2015_basefinal_depen_publicacao.csv.json',
        #  'JSONs/2016_basefinal_depen_publicacao-2016.csv.json',

        jsons_data_datasus = 'JSONs/DATA_SUS_Casos de aids identificados_2014.csv.json'

        jsons_data_populacao = 'JSONs/estimativa_populacao_TCU_2014_20170614.csv.json'
        # 'JSONs/estimativa_populacao_TCU_2015_20170614.csv.json',
        # 'JSONs/estimativa_populacao_TCU_2017_20181108.csv.json',
        # 'JSONs/estimativa_populacao_TCU_2018_20190213.csv.json',
        # 'JSONs/estimativa_TCU_2016_20170614.csv.json'
        list_inserted = []
        # with open(jsons_data_populacao) as json_pop:
        #     with open(jsons_data_infopen) as json_info:
        #         with open(jsons_data_datasus) as json_sus:
        #             data_sus = json.load(json_sus)
        #             data_pop = json.load(json_pop)
        #             data_info = json.load(json_info)
        #             if len(data_pop) >= len(data_info):
        #                 for pop in data_pop:
        #                     to_push = {}
        #                     to_push['pop'] = pop
        #                     to_push['municipip-uf'] = pop['nome-do-municpio'] + "-" + pop['uf']
        #                     print(pop['nome-do-municpio'] + "-" + pop['uf'])
        #                     x = collection.insert_one(to_push)
        #                     print('inserido -> ', x.inserted_id)

        # with open(jsons_data_infopen) as json_info:
        #     data_info = json.load(json_info)
        #     for info in data_info:
        #         x = collection.update({
        #             "municipip-uf": info['cidade']+"-"+info['invite:-uf']},
        #             {"$set": {"info": info}},
        #             upsert=True
        #         )
        #         print(info['cidade']+"-"+info['invite:-uf'])
        #         print('inserido -> ', x)




        with open(jsons_data_datasus) as json_sus:
            data_sus = json.load(json_sus)

            for sus in data_sus:

                k =[x for x in cities_state.citiesStatesDATASUS() if int(str(x['cod'])) == int(sus['id'])]
                if k != None and len(k) >0:
                    sus['uf'] = k[0]['uf']
                    exist = collection.find({"sus": {"exist":False}})
                    x = collection.update({"municipip-uf": sus['municipio-(res)'] + "-" + k[0]['uf'], },
                        {"$set": {"sus": sus}},
                        upsert=True
                    )
                    print(sus['municipio-(res)'])
                    # print('inserido -> ', x)



                        # for pop in data_pop:
                        #     for info in data_info:
                        #         if info['cidade'] == pop['nome-do-municpio'] and info['invite:-uf'] == pop['uf']:
                        #
                        #
                        #             if info['cidade'] == pop['nome-do-municpio'] and info['invite:-uf'] == pop['uf']:
                        #                 if sus['municipio-(res)'] == pop['nome-do-municpio'] and sus['municipio-(res)'] == info['cidade']:
                        #                     if find_filter(list_inserted, info['cidade'] + "-" + pop['uf']):
                        #                         to_push = {}
                        #                         to_push['info'] = info
                        #                         to_push['sus'] = sus
                        #                         to_push['pop'] = pop
                        #                         to_push['municipip-uf'] = info['cidade'] + "-" + pop['uf']
                        #                         list_inserted.append(info['cidade'] + "-" + pop['uf'])
                        #                         x = collection.insert_one(to_push)
                        #                         print('inserido -> ', x.inserted_id)

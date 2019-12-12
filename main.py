from chart import generate
from spider.spider_ibge import Spider
from process.process import Process

def main():
    # Transforma o csv em JSON
    # Spider.scraping(object)
    # ret = Spider.scraping(object)

    #Envia os dados dos JSONs para o Mongo
    # Process.insert_data_mongo(object)
    generate.generateDataset()
    pass



if __name__ == '__main__':
    main()
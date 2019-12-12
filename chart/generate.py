from pymongo import MongoClient
import json
import collections





def generateDataset():
    client = MongoClient("mongodb+srv://root:rq0wKDIdiMryckPn@cluster0-zjn1g.mongodb.net/test?retryWrites=true&w=majority")
    db = client["public"]
    collection = db["infopen_datasus_ibge_2014"]
    skik = 0
    limit = 99
    dataSet=[]
    dataTwo= []
    while len(dataTwo) < 6834:
        dataSet = collection.find({}).skip(0).limit(99)
        for i in dataSet:
            dataTwo.append(i)
        skik += limit
        limit += limit
        print(skik, limit)

    final_data = []
    final = {}
    for data in dataTwo:
        presidio = ""
        populao = ""
        id = ""
        municipipuf = ""
        frequencia = 0
        capacidade = 0
        total = 0
        entradas = 0
        remocoes = 0
        try:
            if type(data['info']['nome-da-unidade-prisional:']) is str:
                presidio = data['info']['nome-da-unidade-prisional:']
        except KeyError or TypeError as e:
            print('chave nao encontrada')
        try:
            if type(data['pop']['populao-estimada']) is str:
                populao = data['pop']['populao-estimada']
            else:
                populao = data['pop']['populao-estimada'][0]
        except KeyError or TypeError as e:
            print('chave nao encontrada')
        try:
            if type(data['sus']['id']) is str:
                id = data['sus']['id']
            else:
                id = data['sus']['id'][0]
        except KeyError or TypeError as e:
            print('chave nao encontrada')
        try:
            if type(data['municipip-uf']) is str:
                municipipuf = data['municipip-uf']
            else:
                municipipuf = data['municipip-uf'][0]
        except KeyError or TypeError as e:
            print('chave nao encontrada')
        try:
            if type(data['sus']['frequencia']) is str:
                frequencia = data['sus']['frequencia']
            else:
                frequencia = data['sus']['frequencia'][0]
        except KeyError or TypeError as e:
            print('chave nao encontrada')
            dict_dataSet = {}

        try:
            if type(data['info']['1-3--capacidade-do-estabelecimento:-|-vagas---regime-semiaberto-|-masculino']) is str:
                if data['info']['1-3--capacidade-do-estabelecimento:-|-vagas---regime-semiaberto-|-masculino'] != '': capacidade = int(data['info']['1-3--capacidade-do-estabelecimento:-|-vagas---regime-semiaberto-|-masculino'] != '')
            else:
                if data['info']['1-3--capacidade-do-estabelecimento:-|-vagas---regime-semiaberto-|-masculino'][0] != '': capacidade = int(data['info']['1-3--capacidade-do-estabelecimento:-|-vagas---regime-semiaberto-|-masculino'][0])
        except KeyError or TypeError as e:
            print('chave invalida')

        try:
            if type(data['info']['1-3--capacidade-do-estabelecimento:-|-vagas---regime-aberto-|-masculino']) is str:
                if data['info']['1-3--capacidade-do-estabelecimento:-|-vagas---regime-aberto-|-masculino'] != '': capacidade += int(data['info']['1-3--capacidade-do-estabelecimento:-|-vagas---regime-aberto-|-masculino'])
            else:
                if data['info']['1-3--capacidade-do-estabelecimento:-|-vagas---regime-aberto-|-masculino'] != '' : capacidade += int(data['info']['1-3--capacidade-do-estabelecimento:-|-vagas---regime-aberto-|-masculino'])
        except KeyError or TypeError as e:
            print('chave invalida')

        try:
            if data['info']['1-3--capacidade-do-estabelecimento:-|-vagas---presos-provisrios-|-masculino'] !='': capacidade += int(data['info']['1-3--capacidade-do-estabelecimento:-|-vagas---presos-provisrios-|-masculino'])
        except KeyError or TypeError as e:
            print('chave invalida')
        try:
            if data['info']['1-3--capacidade-do-estabelecimento:-|-vagas---presos-provisrios-|-feminino'] != '' : capacidade += int(data['info']['1-3--capacidade-do-estabelecimento:-|-vagas---presos-provisrios-|-feminino'])
        except KeyError or TypeError as e:
            print('chave invalida')
        try:
            if data['info']['1-3--capacidade-do-estabelecimento:-|-vagas---regime-fechado-|-masculino'] != '' : capacidade += int(data['info']['1-3--capacidade-do-estabelecimento:-|-vagas---regime-fechado-|-masculino'])
        except KeyError or TypeError  as e:
            print('chave invalida')
        try:
            if data['info']['1-3--capacidade-do-estabelecimento:-|-vagas---regime-fechado-|-feminino'] != '' : capacidade += int(data['info']['1-3--capacidade-do-estabelecimento:-|-vagas---regime-fechado-|-feminino'])
        except KeyError or TypeError as e:
            print('chave invalida')
        try:
            if data['info']['1-3--capacidade-do-estabelecimento:-|-vagas---regime-disciplinar-diferenciado-(rdd)-|-masculino'] != '' : capacidade += int(data['info']['1-3--capacidade-do-estabelecimento:-|-vagas---regime-disciplinar-diferenciado-(rdd)-|-masculino'])
        except KeyError or TypeError as e:
            print('chave invalida')
        try:
            if data['info']['1-3--capacidade-do-estabelecimento:-|-vagas---regime-disciplinar-diferenciado-(rdd)-|-feminino'] != '' : capacidade += int(data['info']['1-3--capacidade-do-estabelecimento:-|-vagas---regime-disciplinar-diferenciado-(rdd)-|-feminino'])
        except KeyError or TypeError as e:
            print('chave invalida')
        try:
            if data['info']['1-3--capacidade-do-estabelecimento:-|-vagas---regime-aberto-|-masculino'][0] != '': capacidade += int(data['info']['1-3--capacidade-do-estabelecimento:-|-vagas---regime-aberto-|-masculino'][0])
        except KeyError or TypeError as e:
            print('chave invalida')
        try:
            if data['info']['1-3--capacidade-do-estabelecimento:-|-vagas---presos-provisrios-|-masculino'][0] != '': capacidade += int(data['info']['1-3--capacidade-do-estabelecimento:-|-vagas---presos-provisrios-|-masculino'][0])
        except KeyError or TypeError as e:
            print('chave invalida')
        try:
            if data['info']['1-3--capacidade-do-estabelecimento:-|-vagas---presos-provisrios-|-feminino'][0] != '' :capacidade += int(data['info']['1-3--capacidade-do-estabelecimento:-|-vagas---presos-provisrios-|-feminino'][0])
        except KeyError or TypeError as e:
            print('chave invalida')
        try:
            if data['info']['1-3--capacidade-do-estabelecimento:-|-vagas---regime-fechado-|-masculino'][0] != '': capacidade += int(data['info']['1-3--capacidade-do-estabelecimento:-|-vagas---regime-fechado-|-masculino'][0])
        except KeyError or TypeError as e:
            print('chave invalida')
        try:
            if data['info']['1-3--capacidade-do-estabelecimento:-|-vagas---regime-fechado-|-feminino'][0] != '' :capacidade += int(data['info']['1-3--capacidade-do-estabelecimento:-|-vagas---regime-fechado-|-feminino'][0])
        except KeyError or TypeError as e:
            print('chave invalida')
        try:
            if data['info']['1-3--capacidade-do-estabelecimento:-|-vagas---regime-disciplinar-diferenciado-(rdd)-|-masculino'][0] != '' : capacidade += int(data['info']['1-3--capacidade-do-estabelecimento:-|-vagas---regime-disciplinar-diferenciado-(rdd)-|-masculino'][0])
        except KeyError or TypeError as e:
            print('chave invalida')
        try:
            if data['info']['1-3--capacidade-do-estabelecimento:-|-vagas---regime-disciplinar-diferenciado-(rdd)-|-feminino'][0] != '': capacidade += int(data['info']['1-3--capacidade-do-estabelecimento:-|-vagas---regime-disciplinar-diferenciado-(rdd)-|-feminino'][0])
        except KeyError or TypeError as e:
            print('chave invalida')
        print(capacidade)
        try:
            if data['info']['4-5-a--entradas-|-nmero-de-incluses-originrias-(incluses-no-decorrentes-de-remoo-ou-transferncia-de-outro-estabelecimento-do-sistema-prisional)-|-masculino'] != '' : entradas = int(data['info']['4-5-a--entradas-|-nmero-de-incluses-originrias-(incluses-no-decorrentes-de-remoo-ou-transferncia-de-outro-estabelecimento-do-sistema-prisional)-|-masculino'])
        except KeyError or TypeError as e:
            print('chave invalida')
        try:
            if data['info']['4-5-a--entradas-|-nmero-de-incluses-originrias-(incluses-no-decorrentes-de-remoo-ou-transferncia-de-outro-estabelecimento-do-sistema-prisional)-|-feminino'] != '': entradas += int(data['info']['4-5-a--entradas-|-nmero-de-incluses-originrias-(incluses-no-decorrentes-de-remoo-ou-transferncia-de-outro-estabelecimento-do-sistema-prisional)-|-feminino'])
        except KeyError or TypeError as e:
            print('chave invalida')
        try:
            if data['info']['4-5-c--transferncias/remoes-|-nmero-de-incluses-por-transferncias-ou-remoes-(recebimento-de-pessoas-privadas-de-liberdade-oriundas-de-outros-estabelecimentos-do-prprio-sistema-prisional)-|-masculino'] != '': remocoes = int(data['info']['4-5-c--transferncias/remoes-|-nmero-de-incluses-por-transferncias-ou-remoes-(recebimento-de-pessoas-privadas-de-liberdade-oriundas-de-outros-estabelecimentos-do-prprio-sistema-prisional)-|-masculino'])
        except KeyError or TypeError as e:
            print('chave invalida')
        try:
            if data['info']['4-5-c--transferncias/remoes-|-nmero-de-incluses-por-transferncias-ou-remoes-(recebimento-de-pessoas-privadas-de-liberdade-oriundas-de-outros-estabelecimentos-do-prprio-sistema-prisional)-|-feminino'] != '': remocoes += int(data['info']['4-5-c--transferncias/remoes-|-nmero-de-incluses-por-transferncias-ou-remoes-(recebimento-de-pessoas-privadas-de-liberdade-oriundas-de-outros-estabelecimentos-do-prprio-sistema-prisional)-|-feminino'])
        except KeyError or TypeError as e:
            print('chave invalida')
        try:
            if data['info']['4-5-c--transferncias/remoes-|-transferncias/-remoes---deste-para-outro-estabelecimento-|-masculino'] != '': remocoes += int(data['info']['4-5-c--transferncias/remoes-|-transferncias/-remoes---deste-para-outro-estabelecimento-|-masculino'])
        except KeyError or TypeError as e:
            print('chave invalida')
        try:
            if data['info']['4-5-c--transferncias/remoes-|-transferncias/-remoes---deste-para-outro-estabelecimento-|-feminino'] != '': remocoes += int(data['info']['4-5-c--transferncias/remoes-|-transferncias/-remoes---deste-para-outro-estabelecimento-|-feminino'])
        except KeyError or TypeError as e:
            print('chave invalida')
        try:
            if data['info']['4-5-b--sadas-|-alvars-de-soltura-(computar-apenas-os-alvars-que-so-efetivamente-cumpridos--motivando-a-colocao-a-pessoa-em-liberdade)-|-masculino'] != '': remocoes += int(data['info']['4-5-b--sadas-|-alvars-de-soltura-(computar-apenas-os-alvars-que-so-efetivamente-cumpridos--motivando-a-colocao-a-pessoa-em-liberdade)-|-masculino'])
        except KeyError or TypeError as e:
            print('chave invalida')
        try:
            if data['info']['4-5-b--sadas-|-alvars-de-soltura-(computar-apenas-os-alvars-que-so-efetivamente-cumpridos--motivando-a-colocao-a-pessoa-em-liberdade)-|-feminino'] != '': remocoes += int(data['info']['4-5-b--sadas-|-alvars-de-soltura-(computar-apenas-os-alvars-que-so-efetivamente-cumpridos--motivando-a-colocao-a-pessoa-em-liberdade)-|-feminino'])
        except KeyError or TypeError as e:
            print('chave invalida')
        try:
            if data['info']['4-5-b--sadas-|-abandonos-(no-retorno-em-sada-temporria)-|-masculino'] != '' :remocoes += int(data['info']['4-5-b--sadas-|-abandonos-(no-retorno-em-sada-temporria)-|-masculino'])
        except KeyError or TypeError as e:
            print('chave invalida')
        try:
            if data['info']['4-5-a--entradas-|-nmero-de-incluses-originrias-(incluses-no-decorrentes-de-remoo-ou-transferncia-de-outro-estabelecimento-do-sistema-prisional)-|-masculino'][0] != '' : entradas = int(data['info']['4-5-a--entradas-|-nmero-de-incluses-originrias-(incluses-no-decorrentes-de-remoo-ou-transferncia-de-outro-estabelecimento-do-sistema-prisional)-|-masculino'][0])
        except KeyError or TypeError as e:
            print('chave invalida')
        try:
            if data['info']['4-5-a--entradas-|-nmero-de-incluses-originrias-(incluses-no-decorrentes-de-remoo-ou-transferncia-de-outro-estabelecimento-do-sistema-prisional)-|-feminino'][0] != '': entradas += int(data['info']['4-5-a--entradas-|-nmero-de-incluses-originrias-(incluses-no-decorrentes-de-remoo-ou-transferncia-de-outro-estabelecimento-do-sistema-prisional)-|-feminino'][0])
        except KeyError or TypeError as e:
            print('chave invalida')
        try:
            if data['info']['4-5-c--transferncias/remoes-|-nmero-de-incluses-por-transferncias-ou-remoes-(recebimento-de-pessoas-privadas-de-liberdade-oriundas-de-outros-estabelecimentos-do-prprio-sistema-prisional)-|-masculino'][0] != '': remocoes = int(data['info']['4-5-c--transferncias/remoes-|-nmero-de-incluses-por-transferncias-ou-remoes-(recebimento-de-pessoas-privadas-de-liberdade-oriundas-de-outros-estabelecimentos-do-prprio-sistema-prisional)-|-masculino'][0])
        except KeyError or TypeError as e:
            print('chave invalida')
        try:
            if data['info']['4-5-c--transferncias/remoes-|-nmero-de-incluses-por-transferncias-ou-remoes-(recebimento-de-pessoas-privadas-de-liberdade-oriundas-de-outros-estabelecimentos-do-prprio-sistema-prisional)-|-feminino'][0] != '': remocoes += int(data['info']['4-5-c--transferncias/remoes-|-nmero-de-incluses-por-transferncias-ou-remoes-(recebimento-de-pessoas-privadas-de-liberdade-oriundas-de-outros-estabelecimentos-do-prprio-sistema-prisional)-|-feminino'][0])
        except KeyError or TypeError as e:
            print('chave invalida')
        try:
            if data['info']['4-5-c--transferncias/remoes-|-transferncias/-remoes---deste-para-outro-estabelecimento-|-masculino'][0] != '': remocoes += int(data['info']['4-5-c--transferncias/remoes-|-transferncias/-remoes---deste-para-outro-estabelecimento-|-masculino'][0])
        except KeyError or TypeError as e:
            print('chave invalida')
        try:
            if data['info']['4-5-c--transferncias/remoes-|-transferncias/-remoes---deste-para-outro-estabelecimento-|-feminino'][0] != '': remocoes += int(data['info']['4-5-c--transferncias/remoes-|-transferncias/-remoes---deste-para-outro-estabelecimento-|-feminino'][0])
        except KeyError or TypeError as e:
            print('chave invalida')
        try:
            if data['info']['4-5-b--sadas-|-alvars-de-soltura-(computar-apenas-os-alvars-que-so-efetivamente-cumpridos--motivando-a-colocao-a-pessoa-em-liberdade)-|-masculino'][0] != '':remocoes += int(data['info']['4-5-b--sadas-|-alvars-de-soltura-(computar-apenas-os-alvars-que-so-efetivamente-cumpridos--motivando-a-colocao-a-pessoa-em-liberdade)-|-masculino'][0])
        except KeyError or TypeError as e:
            print('chave invalida')
        try:
            if data['info']['4-5-b--sadas-|-alvars-de-soltura-(computar-apenas-os-alvars-que-so-efetivamente-cumpridos--motivando-a-colocao-a-pessoa-em-liberdade)-|-feminino'][0] != '':  remocoes += int(data['info']['4-5-b--sadas-|-alvars-de-soltura-(computar-apenas-os-alvars-que-so-efetivamente-cumpridos--motivando-a-colocao-a-pessoa-em-liberdade)-|-feminino'][0])
        except KeyError or TypeError as e:
            print('chave invalida')
        try:
            if data['info']['4-5-b--sadas-|-abandonos-(no-retorno-em-sada-temporria)-|-masculino'][0] != '' :remocoes += int(data['info']['4-5-b--sadas-|-abandonos-(no-retorno-em-sada-temporria)-|-masculino'][0])
        except KeyError or TypeError as e:
            print('chave invalida')


        dict_dataSet = {}
        dict_dataSet['presidio'] = presidio
        dict_dataSet['populao'] = populao
        dict_dataSet['id'] = id
        dict_dataSet['municipipuf'] = municipipuf
        dict_dataSet['frequencia'] = frequencia
        dict_dataSet['capacidade'] = capacidade
        dict_dataSet['entradas'] = entradas
        dict_dataSet['remocoes'] = remocoes
        dict_dataSet['total'] = total
        print(dict_dataSet)

        if len(dict_dataSet) > 0:
            if len(final_data) >= 1:
                last = final_data[-1]
                if last['id'] != dict_dataSet['id']:
                    final_data.append(dict_dataSet)
            else:
                final_data.append(dict_dataSet)
    final.update({'analise': final_data})
    with open('analise_presidios_capacidade_pop_aids.json', 'w') as fp:
        print('json salvo')
        json.dump(final, fp, ensure_ascii=False).encode('utf8')




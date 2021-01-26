# -*- coding: latin-1 -*-

# Importação das librarias necessárias
import os # libraria de interface do systema operativo (utilizado aqui para criar um diretorio)
import re # libraria para encontrar expressoes regulares
import time # libraria de acesso ao tempo e conversões (utilizado para o script esperar x segundos entre ações)
import traceback # libraria de supporte para facilitar o descubrimento de erros no script
from math import ceil # função da libraria math para arredondar valores

import requests # libraria http para obter conteudo online
from bs4 import BeautifulSoup, element # libraria para extrair informação de ficheiros html e xml
import sqlalchemy as db # conjunto de ferramentas sql e mapeador relacional de objetos
import argparse # Parser para opções de linha de comando

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('-cP', '--currentPage', metavar='N', type=int, default=1, help='Page to start collecting from')
parser.add_argument('--submissao', dest='submission', type=str, choices=['Hoje', 'Semana', 'Ano'], help='Data submission time')

parsedArgs = parser.parse_args()

engine = db.create_engine('mysql://root:password@localhost:3306/public')
metadata = db.MetaData()

connection = engine.connect()

curriculos = db.Table('curriculos', metadata, autoload=True, autoload_with=engine)
conhecimentos = db.Table('conhecimentos', metadata, autoload=True, autoload_with=engine)
empresas = db.Table('empresas', metadata, autoload=True, autoload_with=engine)
formacao = db.Table('formacao', metadata, autoload=True, autoload_with=engine)
habilitacoes = db.Table('habilitacoes', metadata, autoload=True, autoload_with=engine)
profissoes = db.Table('areasprofissionais', metadata, autoload=True, autoload_with=engine)

# Definição dos argumentos de pesquisa
args = {'forwardJsp': '', 'action': '1', 'tipo': 'CV', 'regiao_str': '', 'concelho_facet': '', 'freguesia_facet': '', 'dcpp': '', 'ccentro': '', 'zona_mobilidade': '',
        'habilitacao_min': '', 'regime_trabalho': '', 'origem': 'Portugal', 'idioma': '', 'tipo_contrato': '', 'nacionalidade': '',
        'submissao': parsedArgs.submission if parsedArgs.submission else '', 'dataInicio': '',
        'area_profissional': '', 'habilitacao_ordem': '', 'nivel_min': '', 'nivel_max': '', 'area_formacao': '', 'entidade_formadora': '', 'modalidade_formacao': '',
        'saida_profissional': '', 'habilitacoes_acesso': '', 'localidade': '', 'candidatura_imediata': '', 'hasProfile': 'false', 'trabalhoInterior': 'false',
        'currentPage': f'{parsedArgs.currentPage}', 'pageCount': '', 'resultsPerPage': '100', 'resultCount': '', 'pos': '0', 'len': '0', 'origem_option': 'Portugal', 'text': ''}

zones = {}


def getPages():
    url = "http://iefponline.iefp.pt/IEFP/pesquisas/search.do?" + '&'.join([f"{k}={v}" for k, v in args.items()])  # Url da página de pesquisa
    print(f"Page {args['currentPage']}/{args['pageCount']} - {url}")
    response = requests.request("POST", url)  # Obter o conteúdo da página

    soup = BeautifulSoup(response.content, features="html.parser")

    args['resultCount'] = soup.select('body > div.wrapper > div > div.search > form > input[type=hidden]:nth-child(34)')[0].get('value')  # Extrair o número total de resultados da plataforma
    args['pageCount'] = ceil(int(args['resultCount']) / int(args['resultsPerPage']))  # Definição do número de páginas consoante o número de resultados por página
    # #ofertacard_1758682 > div > div > div > div > div > div > a:nth-child(2) > span
    for page in [x.select('span.card-footer-text') for x in soup.select('article')]:  # for loop para os ID's obtidos
        zones[page[1].text] = page[2].text.strip()
        try:
            getData(page[1].text)  # Processar a página
        except Exception as error:
            print('\n'.join(traceback.format_exception(type(error), error, error.__traceback__)))


# Obtenção da data de cada documento
def getData(pageID):
    url = f'http://iefponline.iefp.pt/IEFP/pesquisas/IEFP/pesquisas/detalhe2.do?name=curriculos&nr={pageID}&posAbs=1&nav=true'  # Url do curriculo

    # Ignorar se o curriculo já está disponivel localmente
    ResultProxy = connection.execute(db.select([curriculos]).where(curriculos.columns.id == pageID and curriculos.columns.titulo != ''))
    if ResultProxy.fetchall(): return
    print(url)
    try: response = requests.request('POST', url)  # Obter o conteúdo da página
    except requests.exceptions.ConnectionError: return

    soup = BeautifulSoup(response.content, features='html.parser')

    for div in soup.find_all("div", string='Habilitações, aptidões e competências'): div.decompose()

    second: element.Tag = soup.select("body > section > div > div > div > div > div > div > div.col-xs-12.col-sm-9.cv-detail > div:nth-child(4) > div > div > div.cardBody")[0]

    def formatText(text):  # Função para remover espaços duplicados
        return re.sub("\s+", " ", text).strip()

    def getText(selection):  # Função para extrair o texto de um certo elemento
        try:
            return formatText(list(soup.find_all("div", string=re.compile(selection))[0].parent.children)[-2].text)
        except:
            return ""

    def getList(selection):  # Função obter grupos de elementos
        try:
            return list(soup.find_all("div", string=re.compile(selection))[0].parent.children)[3:-1][0]
        except:
            return []

    # Seleção da informação
    data = {
        "ID": str(pageID),
        "Titulo": soup.find("h2", {'class': 'nomargins'}).next,
        "Zona": zones[pageID],
        "Conhecimentos Específicos": getText("Conhecimentos Específicos:"),
        "Empresas onde Trabalhou": [],
        "Áreas de Interesse": getText("Áreas de Interesse:"),
        "Áreas profissionais que pretende desempenhar": [],
        "Disponibilidade imediata para trabalhar": getText("Disponibilidade imediata para trabalhar:"),
        "Pretende trabalhar a tempo": getText("Pretende trabalhar a tempo:"),
        "Natureza Pretendida": getText("Natureza Pretendida:"),
        "Tempo Prática": getText("Tempo Prática:"),
        "Horário de trabalho preferido": getText("Horário de trabalho preferido:"),
        "Disponibilidade para viajar": getText("Disponibilidade para viajar:"),
        "Mobilidade a nível nacional": [],
        "Habilitações": [],
        "Língua materna": getText('Língua materna:'),
        "Conhecimentos linguísticos": [],
        "Formação Profissional": [],
        "Competências": [x.text for x in second.select("div:nth-child(5) > div > span > ul > li")]
    }

    # Empresas onde Trabalhou
    x = getList("Empresas onde Trabalhou")
    if x:
        for y in x.select('div.row.cv-result-row'):
            temp = ''.join(formatText(y.text).split('EMPRESAS')[1:])
            temp1 = temp.split('DATA INÍCIO')[1]
            temp2 = temp.split('DATA FIM')[1]
            temp3 = temp.split('OBSERVAÇÕES')[1]
            temp = temp.replace('DATA INÍCIO' + temp1, '')
            temp1 = temp1.replace('DATA FIM' + temp2, '')
            temp2 = temp2.replace('OBSERVAÇÕES' + temp3, '')

            data["Empresas onde Trabalhou"].append({
                "Empresa": temp.strip(),
                "Data Início": temp1.strip(),
                "Data Fim": temp2.strip(),
                "Observações": temp3.strip()
            })

    # Áreas profissionais que pretende desempenhar
    x = getList('Áreas profissionais que pretende desempenhar')
    if x:
        for y in x.select('div.row.cv-result-row'):
            temp = formatText(y.text).split('Principal ')[1]
            temp1 = temp.split('Área ')[1]
            temp = temp.replace('Área ' + temp1, '')
            data["Áreas profissionais que pretende desempenhar"].append({
                "Principal": temp.strip(),
                "Área": temp1.strip()
            })

    # Mobilidade a nível nacional
    x = getList('Mobilidade a nível nacional')
    if x:
        for y in x.select('div.row.cv-result-row'):
            temp = formatText(y.text).split('Região ')[1]
            temp1 = temp.split('Concelho ')[1]
            temp2 = temp.split('Prioridade ')[1]
            temp = temp.replace('Concelho ' + temp1, '')
            temp1 = temp1.replace('Prioridade ' + temp2, '')
            data["Mobilidade a nível nacional"].append({
                "Região": temp.strip(),
                "Concelho": temp1.strip(),
                "Prioridade": temp2.strip()
            })

    # Habilitações
    x = getList('Habilitações')
    if x:
        for y in x.select('div.row.cv-result-row'):
            temp = formatText(y.text).split('Habilitação Escolar ')[1]

            temp1 = temp.split('Área Formação')[1]
            temp2 = temp.split('Curso')[1]
            temp3 = temp.split('Nível Qualificação')[1]
            temp4 = temp.split('Estabelecimento Ensino')[1]
            temp5 = temp.split('Ano Conclusão')[1]
            temp6 = temp.split('Classificação Final')[1]
            temp = temp.replace('Área Formação' + temp1, '')
            temp1 = temp1.replace('Curso' + temp2, '')
            temp2 = temp2.replace('Nível Qualificação' + temp3, '')
            temp3 = temp3.replace('Estabelecimento Ensino' + temp4, '')
            temp4 = temp4.replace('Ano Conclusão' + temp5, '')
            temp5 = temp5.replace('Classificação Final' + temp6, '')

            data["Habilitações"].append({
                "Habilitação Escolar": temp.strip(),
                "Área Formação": temp1.strip(),
                "Curso": temp2.strip(),
                "Nível Qualificação": temp3.strip(),
                "Estabelecimento Ensino": temp4.strip(),
                "Ano Conclusão": temp5.strip(),
                "Classificação Final": temp6.strip()
            })

    # Conhecimentos linguísticos
    x = getList('Conhecimentos linguísticos')
    if x:
        for y in x.select('div.row.cv-result-row'):
            temp = formatText(y.text).split('Idioma ')[1]
            temp1 = temp.split('Oralidade')[1]
            temp2 = temp.split('Escrita')[1]
            temp3 = temp.split('Leitura')[1]
            temp = temp.replace('Oralidade' + temp1, '')
            temp1 = temp1.replace('Escrita' + temp2, '')
            temp2 = temp2.replace('Leitura' + temp3, '')

            data["Conhecimentos linguísticos"].append({
                "Idioma": temp.strip(),
                "Oralidade": temp1.strip(),
                "Escrita": temp2.strip(),
                "Leitura": temp3.strip()
            })

    # Formação Profissional
    x = getList('Formação Profissional:')
    if x:
        for y in x.select('div.row.cv-result-row'):
            temp = formatText(y.text).split('Área de Formação ')[1]

            temp1 = temp.split('Entidade')[1]
            temp2 = temp.split('Tempo do Curso')[1]
            temp = temp.replace('Entidade' + temp1, '')
            temp1 = temp1.replace('Tempo do Curso' + temp2, '')

            data["Formação Profissional"].append({
                "Área de Formação": temp.strip(),
                "Entidade": temp1.strip(),
                "Tempo do Curso": temp2.strip(),
            })

    # with open(f'Cv/{pageID}.json', 'w') as f:  # Exportação da informação para ficheiro JSON
    #     json.dump(data, f, indent=4, ensure_ascii=False)
    try:
        if not parsedArgs.submission:
            connection.execute(db.insert(curriculos).values(id=pageID, titulo=data['Titulo'], zona=data['Zona'],
                                                            conhecimentos=data["Conhecimentos Específicos"], date='2020-05-22'))
        else:
            connection.execute(db.insert(curriculos).values(id=pageID, titulo=data['Titulo'], zona=data['Zona'],
                                                            conhecimentos=data["Conhecimentos Específicos"]))
    except:
        connection.execute(db.update(curriculos).where(curriculos.columns.id == pageID).values(titulo=data['Titulo'], zona=data['Zona']))
        return

    for idiomas in data["Conhecimentos linguísticos"]:
        connection.execute(db.insert(conhecimentos).values(idioma=idiomas['Idioma'], id_curriculo=pageID))

    for empresa in data["Empresas onde Trabalhou"]:
        connection.execute(db.insert(empresas).values(empresa=empresa['Empresa'], inicio=empresa['Data Início'], fim=empresa['Data Fim'], funcao=empresa['Observações'], id_curriculo=pageID))

    for training in data["Formação Profissional"]:
        connection.execute(db.insert(formacao).values(area=training['Área de Formação'], entidade=training['Entidade'], tempo=training['Tempo do Curso'], id_curriculo=pageID))

    for skill in data["Habilitações"]:
        connection.execute(db.insert(habilitacoes).values(habilitacao=skill['Habilitação Escolar'], area=skill['Área Formação'], curso=skill['Curso'], nivel=skill['Nível Qualificação'], id_curriculo=pageID))

    for profession in data["Áreas profissionais que pretende desempenhar"]:
        connection.execute(db.insert(profissoes).values(area=profession['Área'], id_curriculo=pageID))


if not os.path.exists('Cv'): os.makedirs('Cv')  # Criação de um folder Cv caso não exista

getPages()  # Obtenção das páginas do website

page = int(args['currentPage'])
while (page := page + 1) < int(args['resultCount']) / int(args['resultsPerPage']):  # Continuar para as próximas páginas até chegar ao fim
    args['currentPage'] = page
    try:
        getPages()
    except:
        args['currentPage'] = page - 1
        time.sleep(5)

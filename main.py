# -*- coding: latin-1 -*-

# Importa��o das librarias necess�rias
import os # libraria de interface do systema operativo (utilizado aqui para criar um diretorio)
import re # libraria para encontrar expressoes regulares
import time # libraria de acesso ao tempo e convers�es (utilizado para o script esperar x segundos entre a��es)
import traceback # libraria de supporte para facilitar o descubrimento de erros no script
from math import ceil # fun��o da libraria math para arredondar valores

import requests # libraria http para obter conteudo online
from bs4 import BeautifulSoup, element # libraria para extrair informa��o de ficheiros html e xml
import sqlalchemy as db # conjunto de ferramentas sql e mapeador relacional de objetos
import argparse # Parser para op��es de linha de comando

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

# Defini��o dos argumentos de pesquisa
args = {'forwardJsp': '', 'action': '1', 'tipo': 'CV', 'regiao_str': '', 'concelho_facet': '', 'freguesia_facet': '', 'dcpp': '', 'ccentro': '', 'zona_mobilidade': '',
        'habilitacao_min': '', 'regime_trabalho': '', 'origem': 'Portugal', 'idioma': '', 'tipo_contrato': '', 'nacionalidade': '',
        'submissao': parsedArgs.submission if parsedArgs.submission else '', 'dataInicio': '',
        'area_profissional': '', 'habilitacao_ordem': '', 'nivel_min': '', 'nivel_max': '', 'area_formacao': '', 'entidade_formadora': '', 'modalidade_formacao': '',
        'saida_profissional': '', 'habilitacoes_acesso': '', 'localidade': '', 'candidatura_imediata': '', 'hasProfile': 'false', 'trabalhoInterior': 'false',
        'currentPage': f'{parsedArgs.currentPage}', 'pageCount': '', 'resultsPerPage': '100', 'resultCount': '', 'pos': '0', 'len': '0', 'origem_option': 'Portugal', 'text': ''}

zones = {}


def getPages():
    url = "http://iefponline.iefp.pt/IEFP/pesquisas/search.do?" + '&'.join([f"{k}={v}" for k, v in args.items()])  # Url da p�gina de pesquisa
    print(f"Page {args['currentPage']}/{args['pageCount']} - {url}")
    response = requests.request("POST", url)  # Obter o conte�do da p�gina

    soup = BeautifulSoup(response.content, features="html.parser")

    args['resultCount'] = soup.select('body > div.wrapper > div > div.search > form > input[type=hidden]:nth-child(34)')[0].get('value')  # Extrair o n�mero total de resultados da plataforma
    args['pageCount'] = ceil(int(args['resultCount']) / int(args['resultsPerPage']))  # Defini��o do n�mero de p�ginas consoante o n�mero de resultados por p�gina
    # #ofertacard_1758682 > div > div > div > div > div > div > a:nth-child(2) > span
    for page in [x.select('span.card-footer-text') for x in soup.select('article')]:  # for loop para os ID's obtidos
        zones[page[1].text] = page[2].text.strip()
        try:
            getData(page[1].text)  # Processar a p�gina
        except Exception as error:
            print('\n'.join(traceback.format_exception(type(error), error, error.__traceback__)))


# Obten��o da data de cada documento
def getData(pageID):
    url = f'http://iefponline.iefp.pt/IEFP/pesquisas/IEFP/pesquisas/detalhe2.do?name=curriculos&nr={pageID}&posAbs=1&nav=true'  # Url do curriculo

    # Ignorar se o curriculo j� est� disponivel localmente
    ResultProxy = connection.execute(db.select([curriculos]).where(curriculos.columns.id == pageID and curriculos.columns.titulo != ''))
    if ResultProxy.fetchall(): return
    print(url)
    try: response = requests.request('POST', url)  # Obter o conte�do da p�gina
    except requests.exceptions.ConnectionError: return

    soup = BeautifulSoup(response.content, features='html.parser')

    for div in soup.find_all("div", string='Habilita��es, aptid�es e compet�ncias'): div.decompose()

    second: element.Tag = soup.select("body > section > div > div > div > div > div > div > div.col-xs-12.col-sm-9.cv-detail > div:nth-child(4) > div > div > div.cardBody")[0]

    def formatText(text):  # Fun��o para remover espa�os duplicados
        return re.sub("\s+", " ", text).strip()

    def getText(selection):  # Fun��o para extrair o texto de um certo elemento
        try:
            return formatText(list(soup.find_all("div", string=re.compile(selection))[0].parent.children)[-2].text)
        except:
            return ""

    def getList(selection):  # Fun��o obter grupos de elementos
        try:
            return list(soup.find_all("div", string=re.compile(selection))[0].parent.children)[3:-1][0]
        except:
            return []

    # Sele��o da informa��o
    data = {
        "ID": str(pageID),
        "Titulo": soup.find("h2", {'class': 'nomargins'}).next,
        "Zona": zones[pageID],
        "Conhecimentos Espec�ficos": getText("Conhecimentos Espec�ficos:"),
        "Empresas onde Trabalhou": [],
        "�reas de Interesse": getText("�reas de Interesse:"),
        "�reas profissionais que pretende desempenhar": [],
        "Disponibilidade imediata para trabalhar": getText("Disponibilidade imediata para trabalhar:"),
        "Pretende trabalhar a tempo": getText("Pretende trabalhar a tempo:"),
        "Natureza Pretendida": getText("Natureza Pretendida:"),
        "Tempo Pr�tica": getText("Tempo Pr�tica:"),
        "Hor�rio de trabalho preferido": getText("Hor�rio de trabalho preferido:"),
        "Disponibilidade para viajar": getText("Disponibilidade para viajar:"),
        "Mobilidade a n�vel nacional": [],
        "Habilita��es": [],
        "L�ngua materna": getText('L�ngua materna:'),
        "Conhecimentos lingu�sticos": [],
        "Forma��o Profissional": [],
        "Compet�ncias": [x.text for x in second.select("div:nth-child(5) > div > span > ul > li")]
    }

    # Empresas onde Trabalhou
    x = getList("Empresas onde Trabalhou")
    if x:
        for y in x.select('div.row.cv-result-row'):
            temp = ''.join(formatText(y.text).split('EMPRESAS')[1:])
            temp1 = temp.split('DATA IN�CIO')[1]
            temp2 = temp.split('DATA FIM')[1]
            temp3 = temp.split('OBSERVA��ES')[1]
            temp = temp.replace('DATA IN�CIO' + temp1, '')
            temp1 = temp1.replace('DATA FIM' + temp2, '')
            temp2 = temp2.replace('OBSERVA��ES' + temp3, '')

            data["Empresas onde Trabalhou"].append({
                "Empresa": temp.strip(),
                "Data In�cio": temp1.strip(),
                "Data Fim": temp2.strip(),
                "Observa��es": temp3.strip()
            })

    # �reas profissionais que pretende desempenhar
    x = getList('�reas profissionais que pretende desempenhar')
    if x:
        for y in x.select('div.row.cv-result-row'):
            temp = formatText(y.text).split('Principal ')[1]
            temp1 = temp.split('�rea ')[1]
            temp = temp.replace('�rea ' + temp1, '')
            data["�reas profissionais que pretende desempenhar"].append({
                "Principal": temp.strip(),
                "�rea": temp1.strip()
            })

    # Mobilidade a n�vel nacional
    x = getList('Mobilidade a n�vel nacional')
    if x:
        for y in x.select('div.row.cv-result-row'):
            temp = formatText(y.text).split('Regi�o ')[1]
            temp1 = temp.split('Concelho ')[1]
            temp2 = temp.split('Prioridade ')[1]
            temp = temp.replace('Concelho ' + temp1, '')
            temp1 = temp1.replace('Prioridade ' + temp2, '')
            data["Mobilidade a n�vel nacional"].append({
                "Regi�o": temp.strip(),
                "Concelho": temp1.strip(),
                "Prioridade": temp2.strip()
            })

    # Habilita��es
    x = getList('Habilita��es')
    if x:
        for y in x.select('div.row.cv-result-row'):
            temp = formatText(y.text).split('Habilita��o Escolar ')[1]

            temp1 = temp.split('�rea Forma��o')[1]
            temp2 = temp.split('Curso')[1]
            temp3 = temp.split('N�vel Qualifica��o')[1]
            temp4 = temp.split('Estabelecimento Ensino')[1]
            temp5 = temp.split('Ano Conclus�o')[1]
            temp6 = temp.split('Classifica��o Final')[1]
            temp = temp.replace('�rea Forma��o' + temp1, '')
            temp1 = temp1.replace('Curso' + temp2, '')
            temp2 = temp2.replace('N�vel Qualifica��o' + temp3, '')
            temp3 = temp3.replace('Estabelecimento Ensino' + temp4, '')
            temp4 = temp4.replace('Ano Conclus�o' + temp5, '')
            temp5 = temp5.replace('Classifica��o Final' + temp6, '')

            data["Habilita��es"].append({
                "Habilita��o Escolar": temp.strip(),
                "�rea Forma��o": temp1.strip(),
                "Curso": temp2.strip(),
                "N�vel Qualifica��o": temp3.strip(),
                "Estabelecimento Ensino": temp4.strip(),
                "Ano Conclus�o": temp5.strip(),
                "Classifica��o Final": temp6.strip()
            })

    # Conhecimentos lingu�sticos
    x = getList('Conhecimentos lingu�sticos')
    if x:
        for y in x.select('div.row.cv-result-row'):
            temp = formatText(y.text).split('Idioma ')[1]
            temp1 = temp.split('Oralidade')[1]
            temp2 = temp.split('Escrita')[1]
            temp3 = temp.split('Leitura')[1]
            temp = temp.replace('Oralidade' + temp1, '')
            temp1 = temp1.replace('Escrita' + temp2, '')
            temp2 = temp2.replace('Leitura' + temp3, '')

            data["Conhecimentos lingu�sticos"].append({
                "Idioma": temp.strip(),
                "Oralidade": temp1.strip(),
                "Escrita": temp2.strip(),
                "Leitura": temp3.strip()
            })

    # Forma��o Profissional
    x = getList('Forma��o Profissional:')
    if x:
        for y in x.select('div.row.cv-result-row'):
            temp = formatText(y.text).split('�rea de Forma��o ')[1]

            temp1 = temp.split('Entidade')[1]
            temp2 = temp.split('Tempo do Curso')[1]
            temp = temp.replace('Entidade' + temp1, '')
            temp1 = temp1.replace('Tempo do Curso' + temp2, '')

            data["Forma��o Profissional"].append({
                "�rea de Forma��o": temp.strip(),
                "Entidade": temp1.strip(),
                "Tempo do Curso": temp2.strip(),
            })

    # with open(f'Cv/{pageID}.json', 'w') as f:  # Exporta��o da informa��o para ficheiro JSON
    #     json.dump(data, f, indent=4, ensure_ascii=False)
    try:
        if not parsedArgs.submission:
            connection.execute(db.insert(curriculos).values(id=pageID, titulo=data['Titulo'], zona=data['Zona'],
                                                            conhecimentos=data["Conhecimentos Espec�ficos"], date='2020-05-22'))
        else:
            connection.execute(db.insert(curriculos).values(id=pageID, titulo=data['Titulo'], zona=data['Zona'],
                                                            conhecimentos=data["Conhecimentos Espec�ficos"]))
    except:
        connection.execute(db.update(curriculos).where(curriculos.columns.id == pageID).values(titulo=data['Titulo'], zona=data['Zona']))
        return

    for idiomas in data["Conhecimentos lingu�sticos"]:
        connection.execute(db.insert(conhecimentos).values(idioma=idiomas['Idioma'], id_curriculo=pageID))

    for empresa in data["Empresas onde Trabalhou"]:
        connection.execute(db.insert(empresas).values(empresa=empresa['Empresa'], inicio=empresa['Data In�cio'], fim=empresa['Data Fim'], funcao=empresa['Observa��es'], id_curriculo=pageID))

    for training in data["Forma��o Profissional"]:
        connection.execute(db.insert(formacao).values(area=training['�rea de Forma��o'], entidade=training['Entidade'], tempo=training['Tempo do Curso'], id_curriculo=pageID))

    for skill in data["Habilita��es"]:
        connection.execute(db.insert(habilitacoes).values(habilitacao=skill['Habilita��o Escolar'], area=skill['�rea Forma��o'], curso=skill['Curso'], nivel=skill['N�vel Qualifica��o'], id_curriculo=pageID))

    for profession in data["�reas profissionais que pretende desempenhar"]:
        connection.execute(db.insert(profissoes).values(area=profession['�rea'], id_curriculo=pageID))


if not os.path.exists('Cv'): os.makedirs('Cv')  # Cria��o de um folder Cv caso n�o exista

getPages()  # Obten��o das p�ginas do website

page = int(args['currentPage'])
while (page := page + 1) < int(args['resultCount']) / int(args['resultsPerPage']):  # Continuar para as pr�ximas p�ginas at� chegar ao fim
    args['currentPage'] = page
    try:
        getPages()
    except:
        args['currentPage'] = page - 1
        time.sleep(5)

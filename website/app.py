import sqlalchemy as db
from flask import Flask, render_template

app = Flask(__name__)

data = {
    'colors': ['rgba(255, 99, 132, 0.2)', 'rgba(54, 162, 235, 0.2)', 'rgba(255, 206, 86, 0.2)', 'rgba(75, 192, 192, 0.2)', 'rgba(153, 102, 255, 0.2)', 'rgba(255,161,66,0.2)'],
}

engine = db.create_engine('mysql+pymysql://root:password@localhost:3306/public')
metadata = db.MetaData()

connection = engine.connect()

# zonas com mais curriculos submitdos
ResultProxy = connection.execute("""select zona, count(zona) from curriculos where zona != '' group by zona order by count(zona) desc;""")
results = ResultProxy.fetchall()
chart1 = {
    'labels': [(x[0] if x[0] else 'Não Definido') for x in results],
    'values': [x[1] for x in results],
}

# zonas com mais habilitações
ResultProxy = connection.execute("""select B.zona, count(A.habilitacao) from habilitacoes A, curriculos B where B.id = A.id_curriculo group by B.zona order by count(A.habilitacao) desc;""")
results = ResultProxy.fetchall()
chart2 = {
    'labels': [(x[0] if x[0] else 'Não Definido') for x in results],
    'values': [x[1] for x in results],
}

# entidades que fazem mais formações
ResultProxy = connection.execute("""select entidade, count(entidade) from formacao group by entidade order by count(entidade) desc limit 500;""")
results = ResultProxy.fetchall()
chart3 = {
    'labels': [(x[0] if x[0] else 'Não Definido') for x in results],
    'values': [x[1] for x in results],
}

# areas profissionais mais procuradas
ResultProxy = connection.execute("""select area, count(area) from areasprofissionais group by area order by count(area) desc;""")
results = ResultProxy.fetchall()
chart4 = {
    'labels': [(x[0] if x[0] else 'Não Definido') for x in results],
    'values': [x[1] for x in results],
}

# habilitações
ResultProxy = connection.execute("""select habilitacao, count(habilitacao) from habilitacoes group by habilitacao order by count(habilitacao) desc;""")
results = ResultProxy.fetchall()
chart5 = {
    'labels': [(x[0] if x[0] else 'Não Definido') for x in results],
    'values': [x[1] for x in results],
}

# Tempo medio em dias que as pessoas trabalham nas empresas
ResultProxy = connection.execute("""select avg(a.a) from (select DATEDIFF(fim, inicio) as a from empresas a group by a.empresa) a;""")
results = ResultProxy.fetchall()
data['averageWorkTime'] = {
    'values': [x[0] for x in results],
}

# Média do numero de empresas em que as pessoas trabalharam
ResultProxy = connection.execute("""SELECT AVG(a.rcount) FROM (select count(*) as rcount FROM empresas r GROUP BY r.id_curriculo) a;""")
results = ResultProxy.fetchall()
data['averageCompanies'] = {
    'values': [x[0] for x in results],
}

@app.route('/')
def index():
    return render_template('index.html', data=data)

@app.route('/chart1')
def _chart1():
    return render_template('charts/barChart.html', data=data, chartData=chart1,
                           tipo="zonas",
                           title="Zonas com mais curriculos submitdos",
                           description='Este gráfico permite verificar qual a zona do pais em que existe maior procura de emprego.')

@app.route('/chart2')
def _chart2():
    return render_template('charts/barChart.html', data=data, chartData=chart2,
                           tipo="zonas",
                           title="Zonas com mais habilitações",
                           description='Este gráfico permite verificar qual a zona do pais em que existe maior procura de emprego.')

@app.route('/chart3')
def _chart3():
    return render_template('charts/barChart.html', data=data, chartData=chart3,
                           tipo="entidades",
                           title="Entidades que fazem mais formações",
                           description='Este gráfico permite verificar quais são as entidades formadoras que fazem mais formações.')

@app.route('/chart4')
def _chart4():
    return render_template('charts/barChart.html', data=data, chartData=chart4,
                           tipo="áreas profissionais",
                           title="Áreas profissionais mais procuradas",
                           description='Este gráfico permite verificar quais as áreas profissionais que tem maior procura de emprego.')

@app.route('/chart5')
def _chart5():
    return render_template('charts/barChart.html', data=data, chartData=chart5,
                           tipo="habilitações",
                           title="Habilitações",
                           description='Este gráfico permite verificar o nivel de habilitações escolares de cada currículo, ou seja, permite ver qual o nivel de escolaridade dos currículos.')

if __name__ == '__main__':
    app.run()

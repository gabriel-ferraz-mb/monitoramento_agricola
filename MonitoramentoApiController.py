from MonitoramentoBusiness import MonitoramentoBusiness

from flask import Flask, jsonify, request

app = Flask(__name__)
business = MonitoramentoBusiness()

usuario ='juliana'
senha_prod ='ehqoyM6&VWLj'
host_homolog ='vps40890.publiccloud.com.br'
host_producao = "186.202.136.178"

global con_prod
con_prod = business.conecta_db(host_producao, '5432','carbon', usuario, senha_prod)
global con_homolog
con_homolog = business.conecta_db(host_homolog, '5432','carbon', usuario, 'juliana@mudar')

@app.route('/get_zarc', methods=['POST'])
def get_zarc():
    try:
        cod = request.json['cod']
        result = business.get_zarc(cod, con_homolog)
        out = result.to_json(orient='records')[1:-1].replace('},{', '} {')
        return jsonify(out)
    except Exception as e:
        print(e)

@app.route('/get_productivity', methods=['POST'])
def get_productivity():
    try:
        cod = request.json['cod']
        result = business.get_produtividade(cod, con_prod)
        out = result.to_json(orient='records')[1:-1].replace('},{', '} {')
        return jsonify(out)
    except Exception as e:
        print(e)

@app.route('/get_soy', methods=['GET'])
def get_soy():
    try:
        result = business.soy()
        out = result.to_json(orient='records')[1:-1].replace('},{', '} {')
        return jsonify(out)
    except Exception as e:
        print(e)

@app.route('/get_cabecalho', methods=['POST'])
def get_cabecalho():
    try:
        cod = request.json['cod']
        result = business.get_produtividade(cod, con_prod)
        out = result.to_json(orient='records')[1:-1].replace('},{', '} {')
        return jsonify(out)
    except Exception as e:
        print(e)

@app.route('/get_soy_year', methods=['POST'])
def get_soy_year():
    try:
        y = request.json['year']
        result = business.get_soy_year(y)
        out = result.to_json(orient='records')[1:-1].replace('},{', '} {')
        return jsonify(out)
    except Exception as e:
        print(e)

@app.route('/get_soil_dataset', methods=['POST'])
def get_soil_dataset():
    try:
        cod = request.json['cod']
        result = business.get_soil_dataset(cod)
        out = result.to_json(orient='records')[1:-1].replace('},{', '} {')
        return jsonify(out)
    except Exception as e:
        print(e)

@app.route('/get_texture', methods=['POST'])
def get_texture():
    try:
        cod = request.json['cod']
        result = business.get_texture(cod)
        out = result.to_json(orient='records')[1:-1].replace('},{', '} {')
        return jsonify(out)
    except Exception as e:
        print(e)
@app.route('/get_temperature', methods=['POST'])
def get_temperature():
    try:
        cod = request.json['cod']
        sd = request.json['start_date']
        ed = request.json['end_date']
        result = business.get_temperature_dataset(sd, ed, cod)
        out = result.to_json(orient='records')[1:-1].replace('},{', '} {')
        return jsonify(out)
    except Exception as e:
        print(e)

@app.route('/get_precipitation', methods=['POST'])
def get_precipitation():
    try:
        cod = request.json['cod']
        sd = request.json['start_date']
        ed = request.json['end_date']
        result = business.get_precipitation_dataset(sd, ed, cod)
        out = result.to_json(orient='records')[1:-1].replace('},{', '} {')
        return jsonify(out)
    except Exception as e:
        print(e)

@app.route('/get_soilmoisture', methods=['POST'])
def get_soilmoisture():
    try:
        cod = request.json['cod']
        sd = request.json['start_date']
        ed = request.json['end_date']
        result = business.get_soilmoisture_dataset(sd, ed, cod)
        out = result.to_json(orient='records')[1:-1].replace('},{', '} {')
        return jsonify(out)
    except Exception as e:
        print(e)

app.run(port=5000, host = 'localhost', debug=True)
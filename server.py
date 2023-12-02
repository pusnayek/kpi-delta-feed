import os
from flask import Flask, send_file, request, jsonify
from cfenv import AppEnv
from hdbcli import dbapi
from sap.cf_logging import flask_logging
import logging
import main
import excel
from io import BytesIO

app = Flask(__name__)
env = AppEnv()
flask_logging.init(app, logging.INFO)

port = int(os.environ.get('PORT', 3000))
@app.route('/')
def hello():
    return "RUN :/execute - to run replication"

@app.route('/log')
def root_route():
    logger = logging.getLogger('btp-py')
    logger.info('Hi')
    return 'OK'

@app.route('/sftp')
def read_sftp():
    return main.check_sftp()

@app.route('/hana')
def dbconnect():
    return main.check_hana()

@app.route('/execute')
def execute():
    main.execute()
    return "Replication done!"

@app.route('/excel/download', methods = ['GET','POST'])
def download():
    payload = request.json
    # wb = excel.build(jsonify(payload))
    wb = excel.build(payload)
    file_stream = BytesIO()
    wb.save(file_stream)
    file_stream.seek(0)
    return send_file(
        file_stream, 
        mimetype = "application/vnd.ms-excel",
        download_name="Kpi.xlsx", 
        as_attachment=True)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port)
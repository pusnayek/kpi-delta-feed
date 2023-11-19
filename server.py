import os
from flask import Flask
from cfenv import AppEnv
from hdbcli import dbapi
from sap.cf_logging import flask_logging
import logging
import main

app = Flask(__name__)
env = AppEnv()
flask_logging.init(app, logging.INFO)

port = int(os.environ.get('PORT', 3000))
@app.route('/')
def hello():
    return "Hello World!"

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
    return "Execution started.."

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port)
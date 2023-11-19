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

hana_service = 'hana'
hana = env.get_service(label=hana_service)

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

@app.route('/read_file')
def read_file():
    try:
        path = os.getcwd() + "/" + "host_key.pub"
        with open(path, 'r') as data:      
            return data
    except Exception as err:
        return err.message

@app.route('/read_path')
def read_path():
    try:
        path = os.getcwd() + "/" + "host_key.pub"
        return path
    except Exception as err:
        return err



@app.route('/dbconnect')
def dbconnect():
    if hana is None:
        return "Can't connect to HANA service '{}' – check service name?".format(hana_service)
    else:
        conn = dbapi.connect(address=hana.credentials['host'],
                             port=int(hana.credentials['port']),
                             user=hana.credentials['user'],
                             password=hana.credentials['password'],
                             encrypt='true',
                             sslTrustStore=hana.credentials['certificate'])

        cursor = conn.cursor()
        # cursor.execute("select CURRENT_UTCTIMESTAMP from DUMMY")
        # ro = cursor.fetchone()
        # cursor.close()
        # conn.close()
        # return "Current time is: " + str(ro["CURRENT_UTCTIMESTAMP"])
        cursor.execute('SELECT TOP 1 * FROM "4B177AEC29CC443F8EB8FDF8E4BB850A"."FT_ASSIGNMENT"')
        ro = cursor.fetchone()
        cursor.close()
        conn.close()
        return "Fetched user value: " + str(ro["USERID"])
    
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port)
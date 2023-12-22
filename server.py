import os
from flask import Flask, send_file, request, jsonify
from cfenv import AppEnv
from sap.cf_logging import flask_logging
import logging
import main
import excel
from io import BytesIO
from io import StringIO
import time
import sys
import pandas as pd
import xlsxwriter
import threading

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
    df, filename = excel.build_df(payload)
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    df.to_excel(writer, startrow = 0, merge_cells = False, index=False, sheet_name = "data")
    writer.close()
    output.seek(0)
    return send_file(
        output, 
        mimetype = "application/vnd.ms-excel",
        download_name="Kpi.xlsx", 
        as_attachment=True)

@app.route('/timer-start', methods = ['GET'])
def start():
    lock = main.timer_lock()
    if lock == False:
        return "Timer already started!"
    
    while lock:
        time.sleep(600)
        sys.stdout.write('\nINFO - TIMER RUNNING---\n')
        main.execute()
    return "Timer started"

@app.route('/timer-unlock', methods = ['GET'])
def unlock():
    lock = main.timer_unlock()
    if lock == True:
        return "Timer unlocked!"
    else:
        return "Failed to unlock timer!"

# def timer_function(name):
#     while True:
#         sys.stdout.write('\nINFO - TIMER RUNNING---{name}\n'.format(name = name))
#         time.sleep(300)
#         main.execute()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port)

    #start the timer
    # timer = threading.Thread(target=timer_function, args=(1,))
    # timer.start()
    # sys.stdout.write('\nINFO - TIMER STARTED---\n')

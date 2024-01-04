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
    return "RUN : Flask started.."

# @app.route('/log')
# def root_route():
#     logger = logging.getLogger('btp-py')
#     logger.info('Hi')
#     return 'OK'

@app.route('/sftp')
def read_sftp():
    return main.check_sftp()

@app.route('/hana')
def dbconnect():
    return main.check_hana()

# @app.route('/execute')
# def execute():
#     main.execute()
#     return "Replication done!"

@app.route('/excel/download', methods = ['GET','POST'])
def download():
    payload = request.json
    df, filename, language = excel.build_df(payload)
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    df.to_excel(writer, startrow = 0, merge_cells = False, index=False, sheet_name = "data")
    #formatting
    workbook  = writer.book
    worksheet = writer.sheets['data']    
    worksheet.autofit()
    if(language.lower() == 'iw'):
        worksheet.right_to_left()
    #close the writer
    writer.close()
    output.seek(0)
    return send_file(
        output, 
        mimetype = "application/vnd.ms-excel",
        download_name=filename, 
        as_attachment=True)


# timer for replication
def timer_task_replicaton(name):
    while True:
        try:
            time.sleep(600)
            sys.stdout.write('\nINFO - TIMER TASK (REPLICATION) RUNNING---{name}\n'.format(name=name))
            main.execute()
        except Exception as err:
            sys.stdout.write('\nERROR - TIMER TASK (REPLICATION) ---{err}\n'.format(err=err))

# timer for tiles update
def timer_task_tiles(name):
    while True:
        time.sleep(900)
        sys.stdout.write('\nINFO - TIMER TASK (TILES) RUNNING---{name}\n'.format(name=name))


# start the timers
timerReplication = threading.Thread(target=timer_task_replicaton, args=(time.time(),))
timerReplication.start()

# timerTiles = threading.Thread(target=timer_task_tiles, args=(time.time(),))
# timerTiles.start()

sys.stdout.write('\nINFO - TIMER STARTED---\n')


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port)
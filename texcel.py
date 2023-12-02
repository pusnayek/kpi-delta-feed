import pandas as pd
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
import dbconnect
import json
from excels import KpiWriter

def read_filter():
    with open("filter.json", 'r') as data:
        return json.loads(data.read())

def build(payload):
    scenario, language = payload["scenario"], payload["langu"]
    select, viewname, groupby = KpiWriter.getScenario(scenario)
    df = dbconnect.read_data(payload, select, viewname, groupby)
    df = KpiWriter.prepare(df, scenario, language)

    wb = KpiWriter.getTemplate(language)
    ws = wb["data"]

    rows = dataframe_to_rows(df, index=False, header=True)
    for r_idx, row in enumerate(rows, 1):
        for c_idx, value in enumerate(row, 1):
            ws.cell(row=r_idx, column=c_idx, value=value)

    wb.save("Competency.xlsx")


payload = read_filter()
build(payload)
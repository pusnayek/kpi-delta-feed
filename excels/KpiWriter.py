import pandas as pd
import json
from texts import Texts
from openpyxl import load_workbook

CONFIG = {}

def read_config():
    global CONFIG
    with open("./excels/excels.json", "r") as jsonfile:
        CONFIG = json.load(jsonfile)
        jsonfile.close()

def getTemplate(language):
    filename = 'Template.xlsx' if language != 'iw' else 'Template_iw.xlsx'
    return load_workbook('./excels/templates/'+filename)

def getScenario(scenario):
    # print(CONFIG[scenario])
    select = ','.join(CONFIG[scenario]["SELECT"])
    groupby = ','.join(CONFIG[scenario]["GROUPBY"])
    return select, CONFIG[scenario]["VIEWNAME"], groupby

def getReplacedColumn(colname, scenario):
    cols = list(filter(lambda item: item["col"] == colname, CONFIG[scenario]["REPLACE_TRANSLATION"]))
    if(len(cols) > 0):
        return cols[0]["replace"]
    else:
        return colname

def prepare(df, scenario, language):
    Texts.init(language)

    # get the file name
    fname_key = "FNAME{scenario}".format(scenario = scenario.upper())
    filename = '{fname}.xlsx'.format(fname = Texts.get(fname_key))
    # filename = Texts.get(fname_key)+'.xlsx'

    # format date columns
    if(CONFIG[scenario]["DATE_FROMAT"]):
        for column in CONFIG[scenario]["DATE_FROMAT"]:
            # df[column] = df[column].dt.strftime('%d-%b-%Y') 
            # if pd.notnull(df[column]) else ''
            df[column] = df[column].map(lambda val: val.strftime('%d-%b-%Y') if pd.notnull(val) else '')

    # format status columns
    if(CONFIG[scenario]["STAT_FORMAT"]):
        for column in CONFIG[scenario]["STAT_FORMAT"]:
            df[column] = df[column].replace([1, 0], [Texts.get('STATUS/TRUE'), Texts.get('STATUS/FALSE')])

    # format boolean columns
    if(CONFIG[scenario]["BOOL_FORMAT"]):
        for column in CONFIG[scenario]["BOOL_FORMAT"]:
            df[column] = df[column].replace(['X', ''], [Texts.get('BOOL/TRUE'), Texts.get('BOOL/FALSE')])

    # format competency type
    if(CONFIG[scenario]["COMPETENCYTYPE"]):
        for column in CONFIG[scenario]["COMPETENCYTYPE"]:
            df[column] = df[column].replace(['Prof Competency', 'Regulation'], [Texts.get('PROFCOMPETENCYTYPE'), Texts.get('REGULCOMPETENCYTYPE')])

    df.columns = list(map(lambda colname: Texts.get(colname), list(df.columns)))
    # df.columns = list(map(lambda colname: Texts.get(getReplacedColumn(colname, scenario)), list(df.columns)))
    # print(df.columns)
    return df, filename

def getFileName(scenario, langu):
    Texts.init(langu)
    # get the file name
    fname_key = "FNAME{scenario}".format(scenario = scenario.upper())
    # filename = '{fname}.xlsx'.format(fname = Texts.get(fname_key))
    filename = Texts.get(fname_key)+'.xlsx'
    return filename


read_config()


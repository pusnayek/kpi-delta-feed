import pandas as pd
import exconfig
from cfenv import AppEnv
from hana_ml.dataframe import ConnectionContext, create_dataframe_from_pandas
import json
from urllib.parse import unquote
from base64 import b64encode, b64decode

SCHEMA = "4B177AEC29CC443F8EB8FDF8E4BB850A"
CONFIG = exconfig.read_config()
conn = None

def connect():
    global conn

    env = AppEnv()
    hana_service = 'hana'
    hana = env.get_service(label=hana_service)
    HOST = hana.credentials['host']
    USER = hana.credentials['user']
    PASS = hana.credentials['password']

    conn = ConnectionContext(
        address=HOST, port=443, 
        user=USER,
        password=PASS, 
        schema=SCHEMA,
        encrypt=True, 
        sslValidateCertificate=False
    ) 

def check_hana_connection():
    global conn
    return conn.hana_version()
    
def read_hana_table(tablename):
    global conn
    df = conn.table(tablename, schema=SCHEMA).collect()
    return df

def write_df_to_hana(df, type):
    global conn
    try:
        df.rename(columns = CONFIG[type]["COLUMNS"], inplace = True)
        for col in CONFIG[type]["NUMBERCOLS"]:
            df[col].fillna(0, inplace = True)
        df.fillna('', inplace = True)

        dfh = create_dataframe_from_pandas(
            conn, df, 
            CONFIG[type]["HANATAB"],
            schema=SCHEMA, 
            force=True,
            append=True,
            replace=False
        )
    except Exception as err:
        return str(err)

def replicate(type):
    global conn
    proc = 'CALL "{schema}"."PROC_TRANS_DATA_FROM_TEMP"(\'{type}\')'.format(schema = SCHEMA, type = type.upper())
    conn.execute_sql(proc)

def read_data(filter, select, viewname, groupby):
    global conn
    whereClause = build_where_clause(filter)    
    sql = 'SELECT {select} FROM "{schema}"."{view}" WHERE {where}'.format(
        select = select, 
        schema = SCHEMA, 
        view = viewname, 
        where = whereClause)
    sql = '{sql} GROUP BY {groupby}'.format(sql = sql, groupby = groupby) if len(groupby) > 0 else sql
    df = conn.sql(sql).collect()
    return df

def build_where_clause(filter):
    pFilter = '( UPPER("ACTING_USERID") = UPPER(\'{user}\') ) AND ( UPPER("ACTOR") = UPPER(\'{mode}\') )'.format(user = filter["user"], mode = filter["mode"])
    if(filter["filters"]):
        for filterItem in filter["filters"]:
            values = ",".join(map(lambda value: '\''+unquote(b64decode(value))+'\'', filterItem["values"]))
            pFilter = pFilter + ' AND ("{name}" IN ({values}))'.format(name = filterItem["name"], values = values)
    return pFilter

# ( UPPER("ACTING_USERID") = UPPER('adminPN') )
# 	AND ( UPPER("ACTOR") = UPPER('RH') )
# 	AND ("DOMAIN" IN ('CARLSROM_ROMANIA', 'Tuborg Pazarlama (2000)'))

# def read_filter():
#     with open("filter.json", 'r') as data:
#         return data.read()

connect()
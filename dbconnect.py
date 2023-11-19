import pandas as pd
import exconfig
from hana_ml.dataframe import ConnectionContext, create_dataframe_from_pandas

HOST = "20ea0196-836f-4951-b486-3cb02c7d7654.hna1.prod-eu10.hanacloud.ondemand.com"
USER = "4B177AEC29CC443F8EB8FDF8E4BB850A_15QCOTISQ0C3ZA05UO299UNLG_RT"
PASS = "Ix3x76M8932ooPLB-GYqX6YAF-zSckVilaEVQ6lZSurUZ_dUc-ZU21fj7x7wKEi2Wq1WEnSRv9_uuPbHk5fc056xy1N_uNeVOCy-dv3nSWyCzFIVb7ksbZwC0xSiSshE"
SCHEMA = "4B177AEC29CC443F8EB8FDF8E4BB850A"

CONFIG = exconfig.read_config()

def read_hana_table():
    conn = ConnectionContext(address=HOST, port=443, user=USER,
                            password=PASS, schema=SCHEMA,
                            encrypt=True, sslValidateCertificate=False) 
    print(conn.hana_version())
    df = conn.table('TT_ITEM', schema=SCHEMA).collect()
    print(df.head(0))

def write_df_to_hana(df, type):
    try:
        conn = ConnectionContext(address=HOST, port=443, user=USER,
                                password=PASS, schema=SCHEMA,
                                encrypt=True, sslValidateCertificate=False) 
        
        df.rename(columns = CONFIG[type]["COLUMNS"], inplace = True)
        for col in CONFIG[type]["NUMBERCOLS"]:
            df[col].fillna(0, inplace = True)
        df.fillna('', inplace = True)
        # print("writing {n} entries to HANA..".format(n = len(df.index)))

        dfh = create_dataframe_from_pandas(conn, df, CONFIG[type]["HANATAB"],
                                schema=SCHEMA, 
                                force=True, # True: truncate and insert
                                append=True,
                                replace=False) # True: Null is replaced by 0

        # print(dfh.describe().collect())
        # dfh.describe().collect()
        # print("written {n} entries to HANA..".format(n = len(dfh.index)))
    except Exception as err:
        print(err)

# read_hana_table()
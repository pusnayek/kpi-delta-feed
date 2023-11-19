import sftp
import dbconnect
import sys

def execute():
    sys.stdout.write("EXECUTION SUMMARY")
    for type, filename, dataframe in sftp.read_files():
        # print(type, filename, len(dataframe.index))
        str = "File type - {type}, name - {name}, length - {len}".format(type = type, name = filename, len = len(dataframe.index))
        sys.stdout.write(str)
        
        str = dbconnect.write_df_to_hana(dataframe, type)
        sys.stdout.write(str)
        # print(dataframe.head(2))
        # sftp.archive(filename)


def check_sftp():
    return sftp.check()

def check_hana():
    return dbconnect.check_hana_connection()

import sftp
import dbconnect
import sys

def execute():
    files = []
    sys.stdout.write("\n-----EXECUTION SUMMARY-----\n")
    for type, filename, dataframe in sftp.read_files():
        str = "File type - {type}, name - {name}, length - {len}".format(type = type, name = filename, len = len(dataframe.index))
        sys.stdout.write(str)
        
        str = dbconnect.write_df_to_hana(dataframe, type)
        str = dbconnect.replicate(type)
        sys.stdout.write("Replication done for {type}".format(type = type))
        files.append(filename)

    for filename in files:        
        sftp.move_file(filename)
        sys.stdout.write("File archived {filename}".format(filename = filename))


def check_sftp():
    return sftp.check()

def check_hana():
    return dbconnect.check_hana_connection()

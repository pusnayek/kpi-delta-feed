import sftp
import dbconnect
import sys

def execute():
    files = []
    sys.stdout.write("\nINFO - EXECUTION SUMMARY-----\n")
    for type, filename, dataframe in sftp.read_files():
        str = "\nINFO - File type - {type}, name - {name}, length - {len}\n".format(type = type, name = filename, len = len(dataframe.index))
        sys.stdout.write(str)
        
        str = dbconnect.write_df_to_hana(dataframe, type)
        str = dbconnect.replicate(type)
        sys.stdout.write("\nINFO - Replication done for {type}\n".format(type = type))
        files.append(filename)

    for filename in files:        
        sftp.move_file(filename)
        sys.stdout.write("\nINFO - File archived {filename}\n".format(filename = filename))

def check_sftp():
    return sftp.check()

def check_hana():
    return dbconnect.check_hana_connection()

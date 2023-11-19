import sftp

def execute():
    str = "hello-"
    for type, filename, dataframe in sftp.read_files():
        # print(type, filename, len(dataframe.index))
        str = str + "File type - {type}, name - {name}, length - {len}".format(type = type, name = filename, len = len(dataframe.index))
        # print(dataframe.head(2))
        # sftp.archive(filename)
    return str

# execute()

def check_sftp():
    return sftp.check()

# check_sftp()
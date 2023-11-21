import paramiko
import pandas as pd
import exconfig
import sys

connection_host = 'prodftp2.successfactors.eu'
connection_user = 'centralbot-stage'
connection_password = '8DfQMHKUwf'
connection_dir='/incoming/LMS'

CONFIG = exconfig.read_config()

def check_file(filename):
    if not filename.endswith(".csv"):
        return False, ''
    for key in CONFIG.keys():
        for pattern in CONFIG[key]["FILENAME-PATTERNS"]:
            if filename.startswith(pattern):
                return True, key
    return False, ''

def read_files():
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy)
        ssh.connect(connection_host, username=connection_user, password=connection_password)
        sftp_client = ssh.open_sftp()

        files = sftp_client.listdir(connection_dir)
        sftp_client.chdir(connection_dir)
        for file in files:
            matches, type = check_file(file)
            if(matches):
                # print(file, type)
                with sftp_client.file(file, 'r') as data:
                    df = pd.read_csv(data)
                    yield type, file, df
    except Exception as err:
        print(err)
    finally:
        if sftp_client:
            sftp_client.close()
        if ssh:
            ssh.close()

def archive(filename):
    global sftp_client, connection_dir
    # old_name = connection_dir + "/" + filename
    # new_name = connection_dir + "/Archive" + "/" + filename
    old_name = filename
    new_name = "/Archive" + "/" + filename
    sys.stdout.write("Archiving file {file}".format(file = old_name))
    sftp_client.rename(old_name, new_name)
    sys.stdout.write("Archived file {file}".format(file = new_name))

def move_file(filename):
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy)
        ssh.connect(connection_host, username=connection_user, password=connection_password)
        sftp_client = ssh.open_sftp()
        # archive(filename)
        # sftp_client.chdir(connection_dir)
        old_name = connection_dir + "/" + filename
        new_name = connection_dir + "/Archive/" + "/" + filename
        sftp_client.rename(old_name, new_name)
        # command = "mv " + connection_dir + "/" + filename + " " + connection_dir + "/Archive"
        # print(command)
        # stdin,stdout,stderr = ssh.exec_command(command)
        # print(stdout.readlines())
        # print(stderr.readlines())
    except Exception as err:
        return str(err)
    finally:
        if sftp_client:
            sftp_client.close()
        if ssh:
            ssh.close()

def check():
    global sftp_client
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy)
        ssh.connect(connection_host, username=connection_user, password=connection_password)
        sftp_client = ssh.open_sftp()

        files = sftp_client.listdir(connection_dir)
        sftp_client.chdir(connection_dir)
        return "{n} files found".format(n = len(files))
    except UnicodeDecodeError as err:
        return "UnicodeDecodeError > "+str(err)
    except Exception as err:
        return "Exception > "+str(err)
    finally:
        if sftp_client:
            sftp_client.close()
        if ssh:
            ssh.close()
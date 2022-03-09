import paramiko
import os
host_name = '34.69.145.125'
user_name = 'agruduser'
key_file = r'D:\\sriram\\agrud\\credentials\\mtwireagrudserver.pem'

def get_sftp_conn():
    try:
        ssh_conn = paramiko.SSHClient()
        ssh_conn.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_conn.connect(hostname=host_name,username=user_name,key_filename=key_file)
        sftp_conn = ssh_conn.open_sftp()
        print(f'successfully connected to host: {host_name}')
        return sftp_conn,ssh_conn
    except Exception as e:
        print(f'Exception: {e}')
        sftp_conn.close()
        ssh_conn.close()
        print("conn's closed")

get_sftp_conn()
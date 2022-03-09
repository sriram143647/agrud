import paramiko
host_name = '34.69.145.125'
user_name = 'agruduser'
key_file = r'D:\\sriram\\agrud\\credentials\\mtwireagrudserver.pem'
remote_path = '/home/agruduser/news_data/'
local_path = r'D:\\sriram\\agrud\\news\\mtwire\\server_connect\\new_xmls\\'
count = 1

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

def main():
    global count
    try:
        sftp_conn,ssh_conn = get_sftp_conn()
        # sftp_conn.chdir('/home/agruduser/news_data')
        # files = sftp_conn.listdir('/home/agruduser/news_data')
        stdin,stdout,stderr = ssh_conn.exec_command('cd /home/agruduser/news_data \n ls')
        for file in stdout.readlines():
            file = file.replace('\n','')
            remote_file = remote_path+file
            local_file = local_path+file
            sftp_conn.stat(remote_file)
            sftp_conn.get(remote_file,local_file)
            print(f'file {count}: {file} download successfully')
            count += 1
        sftp_conn.close()
        ssh_conn.close()
        print("conn's closed")
    except Exception as e:
        print(f'Exception: {e}')
        sftp_conn.close()
        ssh_conn.close()
        print("conn's closed")

main()
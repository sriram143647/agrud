import requests
import zipfile
import io
import datetime

# today_date = datetime.datetime.today().strftime('%Y-%m-%d')
today_date = '2022-03-02'
path = 'D:\\sriram\\agrud\\cbonds_data_entry\\test_files\\zip_files\\datafiles'
zip_file_url = f'https://database.cbonds.info/unloads/agrud_technologies/archive/{today_date}.zip'
r = requests.get(zip_file_url, stream=True,auth=('admin@agrud.com', '123456'))
z = zipfile.ZipFile(io.BytesIO(r.content)) 
z.extractall(path+'\\'+today_date)
print(f'{today_date} file downloaded successfully')
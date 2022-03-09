import io
import requests
import zipfile
import concurrent.futures
import animation
links = [
    'http://database.cbonds.info/unloads/agrud_technologies/archive/quotes_archive_1.zip',
    'http://database.cbonds.info/unloads/agrud_technologies/archive/quotes_archive_2.zip',
    'http://database.cbonds.info/unloads/agrud_technologies/archive/quotes_archive_3.zip',
    'http://database.cbonds.info/unloads/agrud_technologies/archive/quotes_archive_4.zip',
    'http://database.cbonds.info/unloads/agrud_technologies/archive/quotes_archive_5.zip',
    'http://database.cbonds.info/unloads/agrud_technologies/archive/quotes_archive_6.zip',
    'http://database.cbonds.info/unloads/agrud_technologies/archive/quotes_archive_7.zip',
    'http://database.cbonds.info/unloads/agrud_technologies/archive/quotes_archive_8.zip',
]
wait = animation.Wait('elipses')
user = 'admin@agrud.com'
password = '123456'
mod_link = f'https://{user}:{password}@database.cbonds.info/unloads/agrud_technologies/archive/quotes_archive.zip'
file_path = '.\\historical_data'

def file_download(link):
    wait.start()
    res = requests.get(link,stream=True,auth=(user,password))
    zip = zipfile.ZipFile(io.BytesIO(res.content))
    zip.extractall(path=file_path)
    zip.close()
    print('zip download successfully')
    wait.stop()

if __name__ == '__main__':
    with concurrent.futures.ProcessPoolExecutor() as link_executor:
        [link_executor.submit(file_download,link) for link in links]
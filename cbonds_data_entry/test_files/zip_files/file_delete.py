import shutil
from datetime import datetime,timedelta, date

# .strftime('%Y-%m-%d')
start_date = datetime.today().date()
end_date = (start_date - timedelta(days=30))
def get_date_ranges(date1, date2):
    dates = []
    for n in range(int ((date1 - date2).days)+1):
        date = date1 + timedelta(n)
        dates.append(date)
    return dates


dates = get_date_ranges(start_date,end_date)
for dt in dates:
    path = r'D:\\sriram\\agrud\\cbonds_data_entry\\test_files\\zip_files\\data_files\\'
    folder_path = path+date
    shutil.rmtree(folder_path)
    print(f'{date} files deleted successfully')
import datetime
import time
tm_format = '%Y-%m-%d %H:%M:%S'
# time now
tm_now = datetime.datetime.now()
tm_now_str = datetime.datetime.strftime(tm_now,tm_format)

# gmt time converter
def gmt_time(tm):
    tm = datetime.datetime.strptime(tm,tm_format)
    gmt_struct_dt_tm = time.gmtime(time.mktime(tm.timetuple()))
    gmt_dt_tm = datetime.datetime.fromtimestamp(time.mktime(gmt_struct_dt_tm))
    gmt_tm = datetime.datetime.strftime(gmt_dt_tm,"%Y-%m-%d %H:%M:%S")
    return gmt_tm

start_dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
gmt_start_tm = gmt_time(start_dt)
end_dt = (datetime.datetime.now()-datetime.timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S") 
gmt_end_tm = gmt_time(end_dt)
linux_cmd = f'find ~/news_data -newerct "{gmt_end_tm}" -not -newerct "{gmt_start_tm}"'
print(linux_cmd)
import datetime
import time
tm_format = '%Y-%m-%d %H:%M:%S'


# time now
tm_now = datetime.datetime.now()
tm_now_str = datetime.datetime.strftime(tm_now,tm_format)

# gmt time converter
gmt_struct_dt_tm = time.gmtime(time.mktime(tm_now.timetuple()))
gmt_dt_tm = datetime.datetime.fromtimestamp(time.mktime(gmt_struct_dt_tm))
gmt_tm = datetime.datetime.strftime(gmt_dt_tm,"%Y-%m-%d %H:%M:%S")
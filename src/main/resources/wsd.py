#-*- coding: UTF-8 -*-
import sys
from datetime import datetime, timedelta
import MySQLdb
import traceback
import chardet
import json

#if debug:
from WindPy import *
w.start()

url = "rm-uf67ktcrjo69g32viko.mysql.rds.aliyuncs.com"
user = "zzt"
password = "Zzt19980924x"
db = MySQLdb.connect(url, user, password, "wind", charset='utf8' )
cursor = db.cursor()

    # 000001.SZ 20190603 high
def wsd(code, date, wind_column):
    tmp = datetime.strptime(date, "%Y%m%d")
    date = datetime.strftime(tmp, "%Y-%m-%d")
    print(date)
    response = w.wsd(code, wind_column, date, date, "")
    assert (response.ErrorCode == 0 or response.ErrorCode == -40520007), "errorcode error"
    if response.ErrorCode == -40520007:
        print("no data in:", date)
        return
    item = response.Data[0][0]
    if wind_column == 'ipo_date':
        item_tmp = datetime.strptime(str(item), "%Y-%m-%d %H:%M:%S")
        item = datetime.strftime(item_tmp, "%Y%m%d")
    data = {"data": item}
    data_str = json.dumps(data)
    print(data_str)
    return item # float

if __name__ == '__main__':
    # for i in range(len(sys.argv)):
    #    print(sys.argv[i])
    windcode = sys.argv[1]
    date = sys.argv[2]
    wind_column = sys.argv[3]
    wsd(windcode, date, wind_column)
    sys.exit(0)
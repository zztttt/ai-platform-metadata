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

# python edb.py "M0048648", "20200201" "20201101"
def edb(code, start_date, end_date):
    tmp = datetime.strptime(start_date, "%Y%m%d")
    start_date = datetime.strftime(tmp, "%Y-%m-%d")
    tmp = datetime.strptime(end_date, "%Y%m%d")
    end_date = datetime.strftime(tmp, "%Y-%m-%d")
    response = w.edb(code, start_date, end_date)
    assert (response.ErrorCode == 0 or response.ErrorCode == -40520007), "errorcode error"
    if response.ErrorCode == -40520007:
        print(f"no data from {start_date} to {end_date}")
        return
    #print(response)
    # item = response.Data[0][0]
    # cur = response.Times[0]
    datas = [i for i in response.Data[0]]
    times = [datetime.strftime(cur, "%Y%m%d") for cur in response.Times]
    data = {"data": datas, "date": times}
    data_str = json.dumps(data)
    print(data_str)
    return data_str

if __name__ == '__main__':
    # for i in range(len(sys.argv)):
    #    print(sys.argv[i])
    windcode = sys.argv[1]
    start_date = sys.argv[2]
    end_date = sys.argv[3]
    edb(windcode, start_date, end_date)

    # response = w.edb("M0048648", "2020-02-01", "2020-11-01")
    # print(response)
    sys.exit(0)
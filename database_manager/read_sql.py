import pandas as pd
from database_manager.DB_HOME import DB_stock
import QUANTAXIS as QA
import time

def stock_day(code):
    my_time = time.localtime(time.time())
    month = str(my_time.tm_mon).zfill(2)
    day = str(my_time.tm_mday).zfill(2)
    date = "{}-{}-{}".format(my_time.tm_year,my_time.tm_mon,my_time.tm_mday)
    table_name = code.replace('.','_')
    data=QA.QA_fetch_stock_day_adv(code,'2017-01-01',date)
    # md = pd.read_sql('select * from %s;' % table_name,con = DB_stock)
    return data
import pandas as pd
import builder_baostock as bb
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import numpy as np
import pymysql
import talib
from datetime import datetime, date, timedelta
import sqlalchemy


class macd_strategy(object):
    def __init__(self):
        self.tools = bb.get_tools()
        pymysql.install_as_MySQLdb()
        self.engine_daily=create_engine("mysql://root:1qaz!QAZ@localhost:3306/stock?charset=utf8", max_overflow=5)
        self.engine_MACD=create_engine("mysql://root:1qaz!QAZ@localhost:3306/MACD?charset=utf8", max_overflow=5)


    def macd_double(self):
        stocklist = self.tools.sc.get_all_code().code
        stock_pool = pd.DataFrame()
        for targetStock in stocklist:
            table_name = targetStock.replace('.','_')
            try:
                md = pd.read_sql('select * from %s order by date desc limit 15;' % table_name,con = self.engine_MACD)
            except Exception as e:
                continue
            num = 0
            if md.MACD[0]== None:
                continue
            for i in range(len(md.MACD)-1):
                if md.MACD[i]<0 and md.MACD[i+1]>0 and md.DIFF[i]>0 and md.DEA[i]>0:
                    num += 1
                    if num == 1:
                        rd = pd.read_sql('select close from %s where date= "%s";' % (table_name,md.date[i+1]),con = self.engine_daily)
                        first_close = rd.close[0]
                    if num > 1:
                        rd = pd.read_sql('select close from %s where date= "%s";' % (table_name,md.date[i+1]),con = self.engine_daily)
                        end_close = rd.close[0]
                        last_date=datetime.strptime(md.date[i+1],'%Y-%m-%d').date()
                        #print(last_date)
            if num>=2 and (date.today()-last_date).days<=15 and ((first_close-end_close)/first_close).astype('float')<0.05: #
                stock_pool[targetStock] = targetStock
                print(targetStock+'符合条件')
        stock_pool.to_csv("data_home/%s_macd_double.csv" % str(datetime.now().date()))
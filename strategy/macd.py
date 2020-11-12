import pandas as pd
import builder_baostock as bb
from DB_HOME import DB_stock,DB_MACD,DB_stock_index
import numpy as np
import talib
from datetime import datetime, date, timedelta
import sqlalchemy
import sys


class macd(object):
    def __init__(self):
        self.tools = bb.get_tools()
        self.engine_daily=DB_stock
        self.engine_MACD=DB_MACD


    def macd_double(self):
        stocklist = self.tools.sc.get_all_code()
        stock_pool = pd.DataFrame()
        for crl in range(len(stocklist.code)):
            table_name = stocklist.code[crl].replace('.','_')
            if stocklist.type[crl] == '1':
                self.engine_daily = DB_stock
            elif stocklist.type[crl] == '2':
                self.engine_daily = DB_stock_index
            elif stocklist.type[crl] == '3':
                print(table_name+"type=3")
                continue
            try:
                md = pd.read_sql('select * from %s order by date desc limit 15;' % table_name,con = self.engine_MACD)
            except Exception:
                continue
            num = 0
            if md.MACD[0]== None:
                continue
            for i in range(len(md.MACD)-1,0,-1):
                if md.MACD[i]<0 and md.MACD[i-1]>0 and md.DEA[i]>0 and md.DIFF[i]>0:
                    num += 1
                    if num == 1:
                        rd = pd.read_sql('select close from %s where date= "%s";' % (table_name,md.date[i-1]),con = self.engine_daily)
                        first_close = rd.close[0]
                    if num > 1:
                        rd = pd.read_sql('select close from %s where date= "%s";' % (table_name,md.date[i-1]),con = self.engine_daily)
                        end_close = rd.close[0]
                        last_date=datetime.strptime(md.date[i-1],'%Y-%m-%d').date()
                        #print(last_date)
            if num>=2 and (date.today()-last_date).days<=2 and ((first_close-end_close)/first_close).astype('float')<0.05: #
                stock_pool[stocklist.code[crl]] = stocklist.code[crl]
                if stocklist.type[crl] == '1':
                    print(stocklist.code[crl]+'符合条件')
        stock_pool.to_csv("data_home/%s_macd_double.csv" % str(datetime.now().date()))




    def macd_double_near0(self):
        stocklist = self.tools.sc.get_all_code()
        stock_pool = pd.DataFrame()
        for crl in range(len(stocklist.code)):
            table_name = stocklist.code[crl].replace('.','_')
            if stocklist.type[crl] == '1':
                self.engine_daily = DB_stock
            elif stocklist.type[crl] == '2':
                self.engine_daily = DB_stock_index
            elif stocklist.type[crl] == '3':
                print(table_name+"type=3")
                continue
            try:
                md = pd.read_sql('select * from %s order by date desc limit 15;' % table_name,con = self.engine_MACD)
            except Exception:
                continue
            num = 0
            if md.MACD[0]== None:
                continue
            for i in range(len(md.MACD)-1,0,-1):
                if md.MACD[i]<0 and md.MACD[i-1]>0 and 0.1>md.DEA[i]>-0.1 and 0.1>md.DIFF[i]>-0.1:
                    num += 1
                    if num == 1:
                        rd = pd.read_sql('select close from %s where date= "%s";' % (table_name,md.date[i-1]),con = self.engine_daily)
                        first_close = rd.close[0]
                    if num > 1:
                        rd = pd.read_sql('select close from %s where date= "%s";' % (table_name,md.date[i-1]),con = self.engine_daily)
                        end_close = rd.close[0]
                        last_date=datetime.strptime(md.date[i-1],'%Y-%m-%d').date()
                        #print(last_date)
            if num>=2 and (date.today()-last_date).days<=2 and ((first_close-end_close)/first_close).astype('float')<0.05: #
                stock_pool[stocklist.code[crl]] = stocklist.code[crl]
                if stocklist.type[crl] == '1':
                    print(stocklist.code[crl]+'符合条件')
        stock_pool.to_csv("data_home/%s_macd_double.csv" % str(datetime.now().date()))
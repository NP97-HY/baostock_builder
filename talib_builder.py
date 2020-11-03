import pandas as pd
import builder_baostock as bb
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import numpy as np
import pymysql
import talib
from datetime import datetime, date, timedelta
import sqlalchemy


class talib_builder(object):
    def __init__(self):
        self.tools = bb.get_tools()
        pymysql.install_as_MySQLdb()
        self.engine_daily=create_engine("mysql://root:1qaz!QAZ@localhost:3306/stock?charset=utf8", max_overflow=5)
        self.engine_MACD=create_engine("mysql://root:1qaz!QAZ@localhost:3306/MACD?charset=utf8", max_overflow=5)


    def MACD_build(self):
        stocklist = self.tools.sc.get_all_code().code
        for targetStock in stocklist:
            macd = pd.DataFrame()
            table_name = targetStock.replace('.','_')
            try:
                rd = pd.read_sql('select * from %s;' % table_name,con = self.engine_daily)
            except sqlalchemy.exc.ProgrammingError as e:
                continue
            if len(rd)<33:
                continue
            try:
                md = pd.read_sql('select * from %s;' % table_name,con = self.engine_MACD)
                if datetime.strptime(md.date[len(md)-1],'%Y-%m-%d').date() < datetime.strptime(rd.date[len(rd)-1],'%Y-%m-%d').date():
                    item = (datetime.strptime(rd.date[len(rd)-1],'%Y-%m-%d').date() - datetime.strptime(md.date[len(md)-1],'%Y-%m-%d').date()).days
                    macd['date']=rd.date[len(rd)-item-1:]
                    macd['code']=rd.code[len(rd)-item-1:]
                    macd['EMA12'][0]=md.EMA12[len(md)-1]*11/13+rd.close[len(md)]*2/13
                    macd['EMA26'][0]=md.EMA12[len(md)-1]*25/27+rd.close[len(md)]*2/27
                    macd['DIFF'][0]=macd['EMA12'][0]-macd['EMA26'][0]
                    macd['DEA'][0]=md['DEA'][len(md)-1]*0.8-macd['DIFF'][0]*0.2
                    macd['MACD'][0]=(macd['DIFF'][0]-macd['DEA'][0])*2
                    while item>1:
                        macd['EMA12'][len(macd.date)-item+1]=macd['EMA12'][len(macd.date)-item]*11/13+rd.close[len(rd)-item+1]*2/13
                        macd['EMA26'][len(macd.date)-item+1]=macd['EMA26'][len(macd.date)-item]*11/13+rd.close[len(rd)-item+1]*2/13
                        macd['DIFF'][len(macd.date)-item+1]=macd['EMA12'][len(macd.date)-item]-macd['EMA26'][len(macd.date)-item]
                        macd['DEA'][len(macd.date)-item+1]=macd['DEA'][len(macd.date)-item]*0.8-macd['DIFF'][len(macd.date)-item]*0.2
                        macd['MACD'][len(macd.date)-item+1]=(macd['DIFF'][len(macd.date)-item]-macd['DEA'][len(macd.date)-item])*2
                        item -= 1
                else:
                    print(targetStock+"已是最新日期")
                    continue
            except sqlalchemy.exc.ProgrammingError as e:
                macd['date']=rd.date
                macd['code']=rd.code
                macd['EMA12']=talib.EMA(np.array(rd['close']),timeperiod=12)
                macd['EMA26']=talib.EMA(np.array(rd['close']),timeperiod=24)
                macd['DIFF'],macd['DEA'],macd['MACD'] = talib.MACD(np.array(rd['close']),fastperiod=12,
                                                                             slowperiod=26, signalperiod=9)
                macd['MACD']=macd['MACD']*2
            try:
                macd.to_sql(name=table_name,con=self.engine_MACD,
                                            if_exists='append',index=False)
                print(targetStock+"建表完成")
            except Exception as e:
                print(targetStock+"保存数据失败") 
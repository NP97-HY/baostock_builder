import pandas as pd
import builder_baostock as bb
import numpy as np
from database_manager.DB_HOME import DB_stock,DB_MACD,DB_stock_index,DB_MA
import talib
from datetime import datetime, date, timedelta
import sqlalchemy
from functools import wraps


class talib_builder(object):
    def __init__(self,stock):
        self.stocklist = stock.code
        self.stocktype = stock.type
        self.first_index = stock.index.values
        self.engine_daily=DB_stock
        self.engine_MACD=DB_MACD
        self.engine_MA=DB_MA
        self.engine_index=DB_stock_index

    

    def MACD_build(self):
        stocklist = self.stocklist
        stocktype = self.stocktype
        for num in range(self.first_index[0],self.first_index[0]+len(stocklist)):
            macd = pd.DataFrame()
            table_name = stocklist[num].replace('.','_')
            if stocktype[num] == '1':
                engine_X = self.engine_daily
            elif stocktype[num] == '2':
                engine_X = self.engine_index
            elif stocktype[num] == '3':
                print(table_name+"type=3")
                continue
            try:
                rd = pd.read_sql('select * from %s;' % table_name,con = engine_X)
            except sqlalchemy.exc.ProgrammingError:
                continue
            if len(rd)<50:
                continue
            try:
                md = pd.read_sql('select * from %s;' % table_name,con = self.engine_MACD)
                if len(rd) > len(md):
                #datetime.strptime(md.date[len(md)-1],'%Y-%m-%d').date() < datetime.strptime(rd.date[len(rd)-1],'%Y-%m-%d').date():
                    item = len(rd) - len(md)
                    macd['date']=rd.date[len(rd)-item:]
                    macd['code']=rd.code[len(rd)-item:]
                    macd['EMA12']=talib.EMA(np.array(rd.close[len(rd)-item-30:]),timeperiod=12)[-item:]
                    macd['EMA26']=talib.EMA(np.array(rd.close[len(rd)-item-30:]),timeperiod=26)[-item:]
                    a,b,c = talib.MACD(np.array(rd.close[len(rd)-item-50:]),fastperiod=12,
                                                                             slowperiod=26, signalperiod=9)
                    macd['DIFF']=a[-item:]
                    macd['DEA']=b[-item:]
                    macd['MACD']=c[-item:]
                    # macd['DIFF']=macd['EMA12']-macd['EMA26']
                    # macd['DEA']=md['DEA'][len(md)-1]*0.8+macd['DIFF']*0.2
                    # macd['MACD']=(macd['DIFF']-macd['DEA'])*2
                else:
                    print(stocklist[num]+"已是最新日期")
                    continue
            except sqlalchemy.exc.ProgrammingError:
                macd['date']=rd.date
                macd['code']=rd.code
                macd['EMA12']=talib.EMA(np.array(rd['close']),timeperiod=12)
                macd['EMA26']=talib.EMA(np.array(rd['close']),timeperiod=26)
                macd['DIFF'],macd['DEA'],macd['MACD'] = talib.MACD(np.array(rd['close']),fastperiod=12,
                                                                             slowperiod=26, signalperiod=9)
                macd['MACD']=macd['MACD']*2
            try:
                macd.to_sql(name=table_name,con=self.engine_MACD,
                                            if_exists='append',index=False)
                print(stocklist[num]+"数据保存成功")
            except Exception:
                print(stocklist[num]+"保存数据失败") 


    def MA_builder(self):
        stocklist = self.stocklist
        stocktype = self.stocktype
        for num in range(self.first_index[0],self.first_index[0]+len(stocklist)):
            MA = pd.DataFrame()
            table_name = stocklist[num].replace('.','_')
            if stocktype[num] == '1':
                engine_M = self.engine_daily
            elif stocktype[num] == '2':
                engine_M = self.engine_index
            elif stocktype[num] == '3':
                print(table_name+"type=3")
                continue
            try:
                rd = pd.read_sql('select * from %s;' % table_name,con = engine_M)
            except sqlalchemy.exc.ProgrammingError:
                continue
            if len(rd)<5:
                continue
            try:
                md = pd.read_sql('select * from %s;' % table_name,con = self.engine_MA)
                if datetime.strptime(md.date[len(md)-1],'%Y-%m-%d').date() < datetime.strptime(rd.date[len(rd)-1],'%Y-%m-%d').date():
                    item = len(rd) - len(md)
                    MA['date']=rd.date[len(rd)-item:]
                    MA['code']=rd.code[len(rd)-item:]
                    MA['MA5'] = talib.SMA(np.array(rd['close'][len(rd)-item-6:]),timeperiod=5)[4:]
                    if len(rd)>=10:
                        MA['MA10'] = talib.SMA(np.array(rd['close'][len(rd)-item-11:]),timeperiod=10)[9:]
                        if len(rd)>=20:
                            MA['MA20'] = talib.SMA(np.array(rd['close'][len(rd)-item-21:]),timeperiod=20)[19:]
                            if len(rd)>=60:
                                MA['MA60'] = talib.SMA(np.array(rd['close'][len(rd)-item-61:]),timeperiod=60)[59:]
                                if len(rd)>=120:
                                    MA['MA120'] = talib.SMA(np.array(rd['close'][len(rd)-item-121:]),timeperiod=120)[119:]
                                else:
                                    MA['MA120'] = None
                            else:
                                MA['MA60'] = None
                                MA['MA120'] = None
                        else:
                            MA['MA20'] = None
                            MA['MA60'] = None
                            MA['MA120'] = None
                    else:
                        MA['MA10'] = None
                        MA['MA20'] = None
                        MA['MA60'] = None
                        MA['MA120'] = None
                else:
                    print(stocklist[num]+"已是最新日期")
                    continue
            except sqlalchemy.exc.ProgrammingError:
                MA['date']=rd.date
                MA['code']=rd.code
                MA['MA5'] = talib.SMA(np.array(rd['close']),timeperiod=5)
                MA['MA10'] = talib.SMA(np.array(rd['close']),timeperiod=10)
                MA['MA20'] = talib.SMA(np.array(rd['close']),timeperiod=20)
                MA['MA60'] = talib.SMA(np.array(rd['close']),timeperiod=60)
                MA['MA120'] = talib.SMA(np.array(rd['close']),timeperiod=120)
            try:
                MA.to_sql(name=table_name,con=self.engine_MA,
                                            if_exists='append',index=False)
                print(stocklist[num]+"数据保存成功")
            except Exception:
                print(stocklist[num]+"保存数据失败") 
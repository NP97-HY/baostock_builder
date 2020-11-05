import pandas as pd
import builder_baostock as bb
import numpy as np
from DB_HOME import DB_stock,DB_MACD
import talib
from datetime import datetime, date, timedelta
import sqlalchemy


class talib_builder(object):
    def __init__(self,stocklist:list):
        self.tools = bb.get_tools()
        self.stocklist = stocklist
        self.engine_daily=DB_stock
        self.engine_MACD=DB_MACD


    def MACD_build(self):
        stocklist = self.stocklist
        for targetStock in stocklist:
            macd = pd.DataFrame()
            table_name = targetStock.replace('.','_')
            try:
                rd = pd.read_sql('select * from %s;' % table_name,con = self.engine_daily)
            except sqlalchemy.exc.ProgrammingError as e:
                continue
            if len(rd)<50:
                continue
            try:
                md = pd.read_sql('select * from %s;' % table_name,con = self.engine_MACD)
                if datetime.strptime(md.date[len(md)-1],'%Y-%m-%d').date() < datetime.strptime(rd.date[len(rd)-1],'%Y-%m-%d').date():
                    item = len(rd) - len(md)
                    macd['date']=rd.date[len(rd)-item:]
                    macd['code']=rd.code[len(rd)-item:]
                    macd['EMA12']=md.EMA12[len(md)-1]*11/13+rd.close[len(md)-1]*2/13
                    macd['EMA26']=md.EMA12[len(md)-1]*25/27+rd.close[len(md)-1]*2/27
                    macd['DIFF']=macd['EMA12']-macd['EMA26']
                    macd['DEA']=md['DEA'][len(md)-1]*0.8-macd['DIFF']*0.2
                    macd['MACD']=(macd['DIFF']-macd['DEA'])*2
                    print(targetStock)
                    while item>1:
                        macd['EMA12'][len(macd.date)-item+1]=macd['EMA12'][len(macd.date)-item]*11/13+rd.close[len(rd)-item+1]*2/13
                        macd['EMA26'][len(macd.date)-item+1]=macd['EMA26'][len(macd.date)-item]*11/13+rd.close[len(rd)-item+1]*2/13
                        macd['DIFF'][len(macd.date)-item+1]=macd['EMA12'][len(macd.date)-item+1]-macd['EMA26'][len(macd.date)-item+1]
                        macd['DEA'][len(macd.date)-item+1]=macd['DEA'][len(macd.date)-item+1]*0.8-macd['DIFF'][len(macd.date)-item+1]*0.2
                        macd['MACD'][len(macd.date)-item+1]=(macd['DIFF'][len(macd.date)-item+1]-macd['DEA'][len(macd.date)-item+1])*2
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
                print(targetStock+"数据保存成功")
            except Exception as e:
                print(targetStock+"保存数据失败") 
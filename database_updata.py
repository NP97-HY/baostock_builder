import builder_baostock as bb
import pandas as pd
from DB_HOME import DB_stock,DB_stock_index,DB_FW
from datetime import datetime, date, timedelta
import sqlalchemy
import time
    

class database_updata(object):
    def __init__(self,stocklist):
        self.tools = bb.get_tools()
        self.stocklist = stocklist.code
        self.stocktype = stocklist.type
        self.stockstatus = stocklist.status
        self.first_index = stocklist.index.values
        self.engine_stock=DB_stock
        self.engine_index=DB_stock_index
        self.engine_FW=DB_FW


    def updata_db_all(self):
        codelist = self.stocklist
        stocktype = self.stocktype
        status = self.stockstatus
        ymd = []
        for num in range(self.first_index[0],self.first_index[0]+len(codelist)):
            table_name = codelist[num].replace('.','_')
            if stocktype[num] == '1':
                engine_X = self.engine_stock
            elif stocktype[num] == '2':
                engine_X = self.engine_index
            elif stocktype[num] == '3':
                print(table_name+"type=3")
                continue
            try:
                rd = pd.read_sql('select * from %s;' % table_name,con = engine_X)
                if len(rd) != 0:
                    if status[num] == '0':
                        continue
                    if datetime.strptime(rd.date[len(rd)-1],'%Y-%m-%d').date() <= datetime.now().date()-timedelta(days=1):
                        try:
                            ymd = rd.date[len(rd)-1].split("-")
                        except Exception:
                            print(codelist[num]+"获取数据时间失败")
                            continue
                        next_date = date(int(ymd[0]),int(ymd[1]),int(ymd[2]))+timedelta(days = 1)
                        result = self.tools.dw.get_data(start_date=str(next_date),start_date_year=0,
                                            frequency="d",adjustflag="2",stocklist=[codelist[num]])
                        if result == False:
                            print(codelist[num]+'系统数据录入中，稍后录入')
                            continue
                    else:
                        print(codelist[num]+"已是最新日期")
                        continue
                else:
                    print(codelist[num]+"无数据")
                    raise sqlalchemy.exc.ProgrammingError(statement=1, params=1, orig=1)
            except sqlalchemy.exc.ProgrammingError:
                result = self.tools.dw.get_data(start_date="2017-09-04",start_date_year=0,
                                frequency="d",adjustflag="2",stocklist=[codelist[num]])
            try:
                if result == False:
                    print('系统数据录入中，稍后录入')
                    continue
                result[codelist[num]].to_sql(name=table_name,con=engine_X,
                                           if_exists='append',index=False)
            except Exception:
                print(codelist[num]+"保存数据失败") 


    def updata_FW_all(self):
        codelist = self.stocklist
        stocktype = self.stocktype
        status = self.stockstatus
        ymd = []
        for num in range(self.first_index[0],self.first_index[0]+len(codelist)):
            table_name = codelist[num].replace('.','_')
            if stocktype[num] == '1':
                engine_X = self.engine_FW
            elif stocktype[num] == '2':
                continue
            elif stocktype[num] == '3':
                print(table_name+"type=3")
                continue
            if status[num] == '0':
                continue
            try:
                rd = pd.read_sql('select * from %s;' % table_name,con = engine_X)
                if rd != 0 :
                    print(status[num])
                    ymd = rd.YaQ[-1].split("-")
                    diff_year = time.localtime(time.time()).tm_year - ymd[0]
                    if ymd[1]<4:
                        next_qua = ymd[1]+1
                    result = self.tools.fw.get_profit_data(codelist=[codelist[num]],year=diff_year,quarter=next_qua)
                else:
                    raise sqlalchemy.exc.ProgrammingError(statement=1, params=1, orig=1)
            except sqlalchemy.exc.ProgrammingError:
                result = self.tools.fw.get_profit_data(codelist=[codelist[num]])
            try:
                result[codelist[num]].to_sql(name=table_name,con=engine_X,
                                                if_exists='append',index=False)
            except Exception:
                print(codelist[num]+"保存数据失败") 
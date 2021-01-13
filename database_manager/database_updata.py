import builder_baostock as bb
import pandas as pd
from database_manager.DB_HOME import DB_stock,DB_stock_index,DB_FP,DB_FO,DB_FG,DB_FB,DB_FCF,DB_FD
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
        self.engine_FP=DB_FP
        self.engine_FO=DB_FO
        self.engine_FG=DB_FG
        self.engine_FB=DB_FB
        self.engine_FCF=DB_FCF
        self.engine_FD=DB_FD


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
            if status[num] == '0':
                continue
            try:
                rd = pd.read_sql('select * from %s;' % table_name,con = engine_X)
                if len(rd) != 0:
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
                result = self.tools.dw.get_data(start_date="1996-09-16",start_date_year=0,
                                frequency="d",adjustflag="2",stocklist=[codelist[num]])
            try:
                if result == False:
                    print('系统数据录入中，稍后录入')
                    continue
                result[codelist[num]].to_sql(name=table_name,con=engine_X,
                                           if_exists='append',index=False)
            except Exception:
                print(codelist[num]+"保存数据失败") 


    def updata_FW_profit(self):
        from interface import fw_interface
        codelist = self.stocklist
        status = self.stockstatus
        ymd = []
        for num in range(self.first_index[0],self.first_index[0]+len(codelist)):
            table_name = codelist[num].replace('.','_')
            engine_X = self.engine_FP
            if status[num] == '0':
                continue
            try:
                rd = pd.read_sql('select * from %s;' % table_name,con = engine_X)
                if rd.empty:
                    raise sqlalchemy.exc.ProgrammingError(statement=1, params=1, orig=1)
                else:
                    #print(rd.YaQ[len(rd)-1])
                    ymd = rd.YaQ[len(rd)-1].split("-")
                    diff_year = time.localtime(time.time()).tm_year - int(ymd[0])
                    if int(ymd[1])<4:   #4
                        next_qua = int(ymd[1])+1
                    elif diff_year == 0:
                        #print('1')
                        continue
                    else:
                        next_qua = 1
                    result = fw_interface.profit_data([codelist[num]],diff_year,next_qua)
            except sqlalchemy.exc.ProgrammingError:
                result = fw_interface.profit_data([codelist[num]])
            try:
                result[codelist[num]].to_sql(name=table_name,con=engine_X,
                                                if_exists='append',index=False)
            except Exception:
                print(codelist[num]+"保存数据失败") 
                

    def updata_FW_operation(self):
        from interface import fw_interface
        codelist = self.stocklist
        status = self.stockstatus
        ymd = []
        for num in range(self.first_index[0],self.first_index[0]+len(codelist)):
            table_name = codelist[num].replace('.','_')
            engine_X = self.engine_FO
            if status[num] == '0':
                continue
            try:
                rd = pd.read_sql('select * from %s;' % table_name,con = engine_X)
                if rd.empty:
                    raise sqlalchemy.exc.ProgrammingError(statement=1, params=1, orig=1)
                else:
                    print(rd.YaQ[len(rd)-1])
                    ymd = rd.YaQ[len(rd)-1].split("-")
                    diff_year = time.localtime(time.time()).tm_year - int(ymd[0])
                    if int(ymd[1])<4:   #4
                        next_qua = int(ymd[1])+1
                    elif diff_year == 0:
                        print('1')
                        continue
                    else:
                        next_qua = 1
                    result = fw_interface.operation_data([codelist[num]],diff_year,next_qua)
            except sqlalchemy.exc.ProgrammingError:
                result = fw_interface.operation_data([codelist[num]])
            try:
                result[codelist[num]].to_sql(name=table_name,con=engine_X,
                                                if_exists='append',index=False)
            except Exception:
                print(codelist[num]+"保存数据失败") 


    def updata_FW_growth(self):
        from interface import fw_interface
        codelist = self.stocklist
        status = self.stockstatus
        ymd = []
        for num in range(self.first_index[0],self.first_index[0]+len(codelist)):
            table_name = codelist[num].replace('.','_')
            engine_X = self.engine_FG
            if status[num] == '0':
                continue
            try:
                rd = pd.read_sql('select * from %s;' % table_name,con = engine_X)
                if rd.empty:
                    raise sqlalchemy.exc.ProgrammingError(statement=1, params=1, orig=1)
                else:
                    print(rd.YaQ[len(rd)-1])
                    ymd = rd.YaQ[len(rd)-1].split("-")
                    diff_year = time.localtime(time.time()).tm_year - int(ymd[0])
                    if int(ymd[1])<4:   #4
                        next_qua = int(ymd[1])+1
                    elif diff_year == 0:
                        print('1')
                        continue
                    else:
                        next_qua = 1
                    result = fw_interface.growth_data([codelist[num]],diff_year,next_qua)
            except sqlalchemy.exc.ProgrammingError:
                result = fw_interface.growth_data([codelist[num]])
            try:
                result[codelist[num]].to_sql(name=table_name,con=engine_X,
                                                if_exists='append',index=False)
            except Exception:
                print(codelist[num]+"保存数据失败") 


    def updata_FW_balance(self):
        from interface import fw_interface
        codelist = self.stocklist
        status = self.stockstatus
        ymd = []
        for num in range(self.first_index[0],self.first_index[0]+len(codelist)):
            table_name = codelist[num].replace('.','_')
            engine_X = self.engine_FB
            if status[num] == '0':
                continue
            try:
                rd = pd.read_sql('select * from %s;' % table_name,con = engine_X)
                if rd.empty:
                    raise sqlalchemy.exc.ProgrammingError(statement=1, params=1, orig=1)
                else:
                    print(rd.YaQ[len(rd)-1])
                    ymd = rd.YaQ[len(rd)-1].split("-")
                    diff_year = time.localtime(time.time()).tm_year - int(ymd[0])
                    if int(ymd[1])<4:   #4
                        next_qua = int(ymd[1])+1
                    elif diff_year == 0:
                        print('1')
                        continue
                    else:
                        next_qua = 1
                    result = fw_interface.balance_data([codelist[num]],diff_year,next_qua)
            except sqlalchemy.exc.ProgrammingError:
                result = fw_interface.balance_data([codelist[num]])
            try:
                result[codelist[num]].to_sql(name=table_name,con=engine_X,
                                                if_exists='append',index=False)
            except Exception:
                print(codelist[num]+"保存数据失败") 


    def updata_FW_cash_flow(self):
        from interface import fw_interface
        codelist = self.stocklist
        status = self.stockstatus
        ymd = []
        for num in range(self.first_index[0],self.first_index[0]+len(codelist)):
            table_name = codelist[num].replace('.','_')
            engine_X = self.engine_FCF
            if status[num] == '0':
                continue
            try:
                rd = pd.read_sql('select * from %s;' % table_name,con = engine_X)
                if rd.empty:
                    raise sqlalchemy.exc.ProgrammingError(statement=1, params=1, orig=1)
                else:
                    print(rd.YaQ[len(rd)-1])
                    ymd = rd.YaQ[len(rd)-1].split("-")
                    diff_year = time.localtime(time.time()).tm_year - int(ymd[0])
                    if int(ymd[1])<4:   #4
                        next_qua = int(ymd[1])+1
                    elif diff_year == 0:
                        print('1')
                        continue
                    else:
                        next_qua = 1
                    result = fw_interface.cash_flow_data([codelist[num]],diff_year,next_qua)
            except sqlalchemy.exc.ProgrammingError:
                result = fw_interface.cash_flow_data([codelist[num]])
            try:
                result[codelist[num]].to_sql(name=table_name,con=engine_X,
                                                if_exists='append',index=False)
            except Exception:
                print(codelist[num]+"保存数据失败") 


    def updata_FW_dupont(self):
        from interface import fw_interface
        codelist = self.stocklist
        status = self.stockstatus
        ymd = []
        for num in range(self.first_index[0],self.first_index[0]+len(codelist)):
            table_name = codelist[num].replace('.','_')
            engine_X = self.engine_FD
            if status[num] == '0':
                continue
            try:
                rd = pd.read_sql('select * from %s;' % table_name,con = engine_X)
                if rd.empty:
                    raise sqlalchemy.exc.ProgrammingError(statement=1, params=1, orig=1)
                else:
                    print(rd.YaQ[len(rd)-1])
                    ymd = rd.YaQ[len(rd)-1].split("-")
                    diff_year = time.localtime(time.time()).tm_year - int(ymd[0])
                    if int(ymd[1])<4:   #4
                        next_qua = int(ymd[1])+1
                    elif diff_year == 0:
                        print('1')
                        continue
                    else:
                        next_qua = 1
                    result = fw_interface.dupont_data([codelist[num]],diff_year,next_qua)
            except sqlalchemy.exc.ProgrammingError:
                result = fw_interface.dupont_data([codelist[num]])
            try:
                result[codelist[num]].to_sql(name=table_name,con=engine_X,
                                                if_exists='append',index=False)
            except Exception:
                print(codelist[num]+"保存数据失败") 
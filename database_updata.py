import builder_baostock as bb
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime, date, timedelta
import sqlalchemy
import pymysql
    

class database_updata(object):
    def __init__(self):
        self.tools = bb.get_tools()
        pymysql.install_as_MySQLdb()
        self.engine=create_engine("mysql://root:1qaz!QAZ@localhost:3306/stock?charset=utf8", max_overflow=5)


    def updata_db_all(self):
        stocklist = self.tools.sc.get_all_code().code
        ymd = []
        for targetStock in stocklist:
            table_name = targetStock.replace('.','_')
            try:
                rd = pd.read_sql('select * from %s;' % table_name,con = self.engine)
                if len(rd) != 0:
                    if datetime.strptime(rd.date[len(rd)-1],'%Y-%m-%d').date() <= datetime.now().date()-timedelta(days=1):
                        try:
                            ymd = rd.date[len(rd)-1].split("-")
                        except:
                            print(targetStock+"获取数据时间失败")
                            continue
                        next_date = date(int(ymd[0]),int(ymd[1]),int(ymd[2]))+timedelta(days = 1)
                        result = self.tools.dw.get_data(start_date=str(next_date),start_date_year=0,
                                            frequency="d",adjustflag="2",stocklist=[targetStock])
                    else:
                        print(targetStock+"已是最新日期")
                        continue
                else:
                    print(targetStock+"无数据")
                    raise sqlalchemy.exc.ProgrammingError(statement=1, params=1, orig=1)
            except sqlalchemy.exc.ProgrammingError as e:
                result = self.tools.dw.get_data(start_date="2017-09-04",start_date_year=0,
                                frequency="d",adjustflag="2",stocklist=[targetStock])
            try:
                result[targetStock].to_sql(name=table_name,con=self.engine,
                                           if_exists='append',index=False)
            except Exception as e:
                print(targetStock+"保存数据失败") 

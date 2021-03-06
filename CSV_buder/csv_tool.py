import pandas as pd
from .. import builder_baostock as bb
import time


class csv_tool(object):
    def __init__(self):
        self.tools = bb.get_tools()
        self.my_time = time.localtime(time.time())
        month = str(self.my_time.tm_mon).zfill(2)
        day = str(self.my_time.tm_mday).zfill(2)
        self.date = "{}-{}-{}".format(self.my_time.tm_year,month,day)


    def stocks_csv(self,time:str=None):
        stock_list,fields = self.tools.sc.get_all_code(date = time)
        result = pd.DataFrame(stock_list, columns=fields)
        result.to_csv("../data_home/%s_all_stock.csv" % self.date)


    def stock_data_csv(self,stocklist):
        data,result = self.tools.dw.get_data(stocklist=stocklist)
        for i in stocklist:
            rs = pd.DataFrame(stock_list, columns=fields)
            rs.to_csv("../data_home/%s.csv" % i)


    def macroeconomic_data_csv(self)
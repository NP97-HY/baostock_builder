import pandas as pd
import time


class stock_catcher(object):
    def __init__(self,bs,date:str=None):
        self.bs = bs
        if date == None:
            self.my_time = time.localtime(time.time())
            month = str(self.my_time.tm_mon).zfill(2)
            day = str(self.my_time.tm_mday).zfill(2)
            self.date = "{}-{}-{}".format(self.my_time.tm_year,month,day)
        else:
            self.date = date

    def get_all_code(self,save=False):
        """
        input:date(str),example "2020-8-28"
        """
        stock_list = []
        st = self.bs.query_stock_basic()
        while(st.error_code == "0") and st.next():
            stock_list.append(st.get_row_data())
        result = pd.DataFrame(stock_list, columns=st.fields)
        if save == True:
            result.to_csv("data_home/%s_all_stock.csv" % self.date, encoding="gbk", index=False)
        return result
        

    def isTardeDay(self,start_day:str= None,end_day:str=None):
        """
        input:date(str),example "2020-8-28"
        """
        if end_day == None:
            end_day = self.date
        elif start_day == None:
            start_day = end_day
        stock_list = []
        isday = self.bs.query_trade_dates(start_day,end_day)
        while(isday.error_code == "0") and isday.next():
            stock_list.append(isday.get_row_data())
        return stock_list,isday.fields


    def basic_of_stock(self,code,code_name):
        stock_list = []
        rs = self.bs.query_stock_basic(code=code,code_name=code_name)
        while (rs.error_code == "0") and rs.next():
            stock_list.append(rs.get_row_data())
        return stock_list,rs.fields
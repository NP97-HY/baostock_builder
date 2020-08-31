import baostock as bs
import pandas as pd
import time


class data_Warehouse(object):
    """
    get stock daily data
    """
    def __init__(self):
        lg = bs.login()
        if lg.error_code != "0":
            print("LOGIN FAILED:"+lg.error_msg)

        
    def get_data(self,date="1",start_date=None,frequency="d",adjustflag="2",stocklist:list=None):
        if stocklist == None:
            raise EnvironmentError("no stocklist")
        if date == "1":
            self.my_time = time.localtime(time.time())
            month = str(self.my_time.tm_mon).zfill(2)
            day = str(self.my_time.tm_mday).zfill(2)
            self.date = "{}-{}-{}".format(self.my_time.tm_year,month,day)
        else:
            self.date = date
        if start_date == None:
            self.start_date = "{}-{}-{}".format(self.my_time.tm_year-5,month,day)
        else:
            self.start_date = start_date
        self.frequency = frequency
        self.adjustflag = adjustflag
        return self._query_history_data_list(stocklist=stocklist)


    def _query_history_data_list(self,stocklist):
        """
        get history data for a group stocks
        targetStock : your aim stock,like: ["600001","000002"]
        """
        stock_data_list = {}
        for targetStock in stocklist:
            rs = bs.query_history_k_data_plus(targetStock,
                                        "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST",
                                        start_date=self.start_date, end_date=self.date,
                                        frequency=self.frequency, adjustflag=self.adjustflag)
            if rs.error_code != "0":
                print("GET {} DATA FAILED:".format(targetStock)+rs.error_msg)
                return 0
            datalist = []
            while (rs.error_code == "0") & rs.next():
                datalist.append(rs.get_row_data())
            stock_data_list[targetStock] = datalist
        return stock_data_list,rs.fields

    
    #def 
            
    
        
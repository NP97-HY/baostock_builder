import baostock as bs
import pandas as pd


class data_Warehouse(object):
    """
    get stock daily data
    """
    def __init__(self,stocknum="000001",stocklist=None,date="1",start_date="2017-07-01",frequency="d",adjustflag="2"):
        lg = bs.login()
        self.stocklist = stocklist
        self.date = date
        self.start_date = start_date
        self.frequency = frequency
        self.adjustflag = adjustflag
        if lg.error_code != 0:
            print("LOGIN FILED:"+lg.error_msg)
        if stocklist == None:
            self.data, = _query_history_data(stocknum)
        else:
            self.data = _query_history_data_list(stocklist)
        


    def _query_history_data(self,targetStock): 
        """
        get history data
        targetStock : your aim stock,like: "600001"
        """
        rs = bs.query_history_k_data_plus(targetStock,
                                        "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST",
                                        start_date=self.start_date, end_date=self.date,
                                        frequency=self.frequency, adjustflag=self.adjustflag)
        if rs.error_code != 0:
            print("GET {} DATA FAILED:".format(targetStock)+rs.error_msg)
            return 0
        datalist = []
        while rs.error_code == 0 & rs.next():
            datalist.append(rs.get_row_data())
        result = pd.DataFrame(datalist,columns=rs.fields)
        return datalist,result


    def _query_history_data_list(self,stocklist):
        """
        get history data for a group stocks
        targetStock : your aim stock,like: ["600001","000002"]
        """
        for targetStock in stocklist:
            rs = bs.query_history_k_data_plus(targetStock,
                                        "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST",
                                        start_date=self.start_date, end_date=self.date,
                                        frequency=self.frequency, adjustflag=self.adjustflag)
            if rs.error_code != 0:
                print("GET {} DATA FAILED:".format(targetStock)+rs.error_msg)
                return 0
            datalist = []
            stock_data_list = []
            while rs.error_code == 0 & rs.next():
                datalist.append(rs.get_row_data())
            stock_data_list[targetStock] = datalist
        return stock_data_list


class 
    
            
    
        
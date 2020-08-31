import pandas as pd
import time


class data_Warehouse(object):
    """
    get stock daily data
    """
    def __init__(self,bs):
        self.bs = bs


    def get_data(self,date="1",start_date:int=5,frequency="d",adjustflag="2",stocklist:list=None,save=False):
        if stocklist == None:
            raise EnvironmentError("no stocklist")
        if date == "1":
            self.my_time = time.localtime(time.time())
            month = str(self.my_time.tm_mon).zfill(2)
            day = str(self.my_time.tm_mday).zfill(2)
            self.date = "{}-{}-{}".format(self.my_time.tm_year,month,day)
        else:
            self.date = date
        self.start_date = "{}-{}-{}".format(self.my_time.tm_year-start_date,month,day)
        self.frequency = frequency
        self.adjustflag = adjustflag
        return self._query_history_data_list(stocklist=stocklist,save=save)


    def _query_history_data_list(self,stocklist,save):
        """
        get history data for a group stocks
        targetStock : your aim stock,like: ["sh.600300","sh.601987"]
        """
        stock_data_list = {}
        for targetStock in stocklist:
            rs = self.bs.query_history_k_data_plus(targetStock,
                                        "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST",
                                        start_date=self.start_date, end_date=self.date,
                                        frequency=self.frequency, adjustflag=self.adjustflag)
            if rs.error_code != "0":
                print("GET {} DATA FAILED:".format(targetStock)+rs.error_msg)
                return 0
            datalist = []
            while (rs.error_code == "0") & rs.next():
                datalist.append(rs.get_row_data())
            result = pd.DataFrame(datalist, columns=rs.fields)
            stock_data_list[targetStock] = result
            if save == True:
                re = pd.DataFrame(result, columns=rs.fields)
                re.to_csv("data_home/%s.csv" % targetStock, encoding="gbk", index=False)
        return stock_data_list

    
    #def 
            
    
        
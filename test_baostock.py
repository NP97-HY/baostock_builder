import baostock as bs
import pandas
import time

class baostock_builder(object):
    def __init__(self,date="1",frequency="d",adjustflag="2"):
        if date == "1":
            my_time = time.localtime(time.time())
            month = my_time.tm_mon.zfill(2)
            day = my_time.tm_mday.zfill(2)
            self.date = "{}-{}-{}".format(my_time.tm_year,month,day)
        else:
            self.date = date
        self.frequency = frequency
        self.adjustflag = adjustflag


    def bs_login(self):
        """
        login baostock
        """
        lg = bs.login()
        if lg.error_code != 0:
            print("LOGIN FILED:"+lg.error_msg)
    
    def bs_logout(self):
        """
        logout baostock
        """
        lg = bs.logout()
        if lg.error_code != 0:
            print("LOGIN FILED:"+lg.error_msg)

    def query_history_data(self,targetStock):
        """
        get history data
        targetStock : your aim stock,like: "600001"
        """
        rs = bs.query_history_k_data_plus(targetStock,
                                        "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST",
                                        start_date='2017-07-01', end_date=self.date,
                                        frequency=self.frequency, adjustflag=self.adjustflag)
        if rs.error_code != 0:
            print("GET {} DATA FAILED:".format(targetStock)+rs.error_msg)
            return 0
        data
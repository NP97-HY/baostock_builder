import baostock as bs
import pandas
import time

class baostock_builder(object):
    def __init__(self):
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


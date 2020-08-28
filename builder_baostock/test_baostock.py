import baostock as bs
import pandas
import time
from Data_warehouse import data_Warehouse as dw

class baostock_builder(object):
    def __init__(self,stock:str="sh.600333",stocklist:list=None):
        if stocklist != None:
            self.dw = dw(stocklist=stocklist)
        else:
            self.dw = dw(stocknum=stock)


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


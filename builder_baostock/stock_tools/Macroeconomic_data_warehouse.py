import baostock as bs
import time


class macroeconomic_Data_Warehouse(object):
    def __init__(self,years:int=10,end_date:str=None):
        lg = bs.login()
        self.my_time = time.localtime(time.time())
        if end_date == None:
            month = str(self.my_time.tm_mon).zfill(2)
            day = str(self.my_time.tm_mday).zfill(2)
            self.end_date = "{}-{}-{}".format(self.my_time.tm_year,month,day)
        else:
            self.end_date = end_date
        self.start_date = "{}-{}-{}".format(self.my_time.tm_year-years,month,day)

    def get_deposit_rate(self,start_date:str=None,end_date:str=None):
        """
        存款利率
        """
        if end_date == None:
            end_date = self.end_date
        elif start_day == None:
            start_day = start_date
        stock_list = []
        rs = bs.query_deposit_rate_data(start_date,end_date)
        while(rs.error_code == "0") and rs.next():
            stock_list.append(rs.get_row_data())
        return stock_list,rs.fields

    def get_loan_rate(self,start_date:str=None,end_date:str=None):
        """
        贷款利率
        """
        if end_date == None:
            end_date = self.end_date
        elif start_day == None:
            start_day = start_date
        stock_list = []
        rs = bs.query_loan_rate_data(start_date,end_date)
        while(rs.error_code == "0") and rs.next():
            stock_list.append(rs.get_row_data())
        return stock_list,rs.fields

    def get_required_reserve_ratio_data(self,start_date:str=None,end_date:str=None,yearType="0"):
        """
        存款准备金率
        """
        if end_date == None:
            end_date = self.end_date
        elif start_day == None:
            start_day = start_date
        stock_list = []
        rs = bs.query_required_reserve_ratio_data(start_date,end_date,yearType)
        while(rs.error_code == "0") and rs.next():
            stock_list.append(rs.get_row_data())
        return stock_list,rs.fields

    def get_money_supply_data(self,start_date:str=None,end_date:str=None):
        """
        货币供应量
        """
        if end_date == None:
            end_date = self.end_date
        elif start_day == None:
            start_day = start_date
        stock_list = []
        rs = bs.query_money_supply_data_month(start_date,end_date)
        while(rs.error_code == "0") and rs.next():
            stock_list.append(rs.get_row_data())
        return stock_list,rs.fields

    def get_money_supply_data(self,start_date:str=None,end_date:str=None):
        """
        货币供应量(年底余额)
        """
        if end_date == None:
            end_date = self.end_date
        elif start_day == None:
            start_day = start_date
        stock_list = []
        rs = bs.query_money_supply_data_year(start_date,end_date)
        while(rs.error_code == "0") and rs.next():
            stock_list.append(rs.get_row_data())
        return stock_list,rs.fields

    def get_shibor_data(self,start_date:str=None,end_date:str=None):
        """
        银行间同业拆放利率
        """
        if end_date == None:
            end_date = self.end_date
        elif start_day == None:
            start_day = start_date
        stock_list = []
        rs = bs.query_shibor_data(start_date,end_date)
        while(rs.error_code == "0") and rs.next():
            stock_list.append(rs.get_row_data())
        return stock_list,rs.fields
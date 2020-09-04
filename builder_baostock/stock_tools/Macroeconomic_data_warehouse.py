import pandas as pd
import time


class macroeconomic_Data_Warehouse(object):
    def __init__(self,bs,years:int=10,end_date:str=None):
        self.bs = bs
        self.my_time = time.localtime(time.time())
        if end_date == None:
            month = str(self.my_time.tm_mon).zfill(2)
            day = str(self.my_time.tm_mday).zfill(2)
            self.end_date = "{}-{}-{}".format(self.my_time.tm_year,month,day)
        else:
            self.end_date = end_date
        self.start_date = "{}-{}-{}".format(self.my_time.tm_year-years,month,day)



    def get_deposit_rate(self,start_date:str=None,end_date:str=None,save=True):
        """
        存款利率
        """
        if end_date == None:
            end_date = self.end_date
        if start_day == None:
            start_day = self.start_date
        data_saver = []
        rs = self.bs.query_deposit_rate_data(start_date,end_date)
        while(rs.error_code == "0") and rs.next():
            data_saver.append(rs.get_row_data())
        result = pd.DataFrame(data_saver, columns=rs.fields)
        if save == True:
            result.to_csv("data_home/deposit_rate.csv")
        return result

    def get_loan_rate(self,start_date:str=None,end_date:str=None,save=True):
        """
        贷款利率
        """
        if end_date == None:
            end_date = self.end_date
        if start_day == None:
            start_day = self.start_date
        data_saver = []
        rs = self.bs.query_loan_rate_data(start_date,end_date)
        while(rs.error_code == "0") and rs.next():
            data_saver.append(rs.get_row_data())
        result = pd.DataFrame(data_saver, columns=rs.fields)
        if save == True:
            result.to_csv("data_home/loan_rate.csv")
        return result

    def get_required_reserve_ratio_data(self,start_date:str=None,end_date:str=None,yearType="0",save=True):
        """
        存款准备金率
        """
        if end_date == None:
            end_date = self.end_date
        if start_day == None:
            start_day = self.start_date
        data_saver = []
        rs = self.bs.query_required_reserve_ratio_data(start_date,end_date,yearType)
        while(rs.error_code == "0") and rs.next():
            data_saver.append(rs.get_row_data())
        result = pd.DataFrame(data_saver, columns=rs.fields)
        if save == True:
            result.to_csv("data_home/required_reserve_ratio_data.csv", encoding="gbk", index=False)
        return result

    def get_money_supply_data_month(self,start_date:str=None,end_date:str=None,save=True):
        """
        货币供应量
        """
        if end_date == None:
            end_date = self.end_date
        if start_day == None:
            start_day = self.start_date
        data_saver = []
        rs = self.bs.query_money_supply_data_month(start_date,end_date)
        while(rs.error_code == "0") and rs.next():
            data_saver.append(rs.get_row_data())
        result = pd.DataFrame(data_saver, columns=rs.fields)
        if save == True:
            result.to_csv("data_home/money_supply_data.csv", encoding="gbk", index=False)
        return result

    def get_money_supply_data_year(self,start_date:str=None,end_date:str=None,save=True):
        """
        货币供应量(年底余额)
        """
        if end_date == None:
            end_date = self.end_date
        if start_day == None:
            start_day = self.start_date
        data_saver = []
        rs = self.bs.query_money_supply_data_year(start_date,end_date)
        while(rs.error_code == "0") and rs.next():
            data_saver.append(rs.get_row_data())
        result = pd.DataFrame(data_saver, columns=rs.fields)
        if save == True:
            result.to_csv("data_home/money_supply_data.csv", encoding="gbk", index=False)
        return result

    def get_shibor_data(self,start_date:str=None,end_date:str=None,save=True):
        """
        银行间同业拆放利率
        """
        if end_date == None:
            end_date = self.end_date
        if start_day == None:
            start_day = self.start_date
        data_saver = []
        rs = self.bs.query_shibor_data(start_date,end_date)
        while(rs.error_code == "0") and rs.next():
            data_saver.append(rs.get_row_data())
        result = pd.DataFrame(data_saver, columns=rs.fields)
        if save == True:
            result.to_csv("data_home/shibor_data.csv", encoding="gbk", index=False)
        return result
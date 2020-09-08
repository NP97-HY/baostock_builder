import builder_baostock as bb
import pandas as pd


class data_catcher(object):
    
    def __init__(self):
        self.builder = bb.get_tools()


    def all_stock_code(self):
        code = self.builder.sc.get_all_code()
        return code[code]


    def trade_data_day(self,stockcode,start_date_year=1,start_date_month=0):
        return self.builder.dw.get_data(start_date_year=start_date_year,start_date_month=start_date_month,stocklist=stockcode)
        

    def sz_50_index(self,save=False):
        return self.builder.ic.get_sz_50_index(save=save)


    def stock_industry(self,save=False):
        return self.builder.ic.get_stock_industry(save=save)


    def hs_300_index(self,save=False):
        return self.builder.ic.get_hs_300_index(save=save)


    def zz_500_index(self,save=False):
        return self.builder.ic.get_zz_500_index(save=save)


    def profit_data(self,codelist=None,save=False):
        return self.builder.fw.get_profit_data(codelist=codelist,save=save)


    def operation_data(self,codelist=None,save=False):
        return self.builder.fw.get_operation_data(codelist=codelist,save=save)


    def growth_data(self,codelist=None,save=False):
        return self.builder.fw.get_growth_data(codelist=codelist,save=save)


    def balance_data(self,codelist=None,save=False):
        return self.builder.fw.get_balance_data(codelist=codelist,save=save)


    def cash_flow_data(self,codelist=None,save=False):
        return self.builder.fw.get_cash_flow_data(codelist=codelist,save=save)


    def dupont_data(self,codelist=None,save=False):
        return self.builder.fw.get_dupont_data(codelist=codelist,save=save)


    def performance_express_report(self,codelist=None,save=False):
        return self.builder.fw.get_performance_express_report(codelist=codelist,save=save)


    def forcast_report(self,codelist=None,save=False):
        return self.builder.fw.get_forcast_report(codelist=codelist,save=save)


    def shibor_data(self,start_date=None,end_date=None,save=False):
        return self.builder.mdw.get_shibor_data(start_date=start_date,end_date=end_date,save=save)


    def money_supply_data_year(self,start_date=None,end_date=None,save=False):
        return self.builder.mdw.get_money_supply_data_year(start_date=start_date,end_date=end_date,save=save)


    def money_supply_data_month(self,start_date=None,end_date=None,save=False):
        return self.builder.mdw.get_money_supply_data_month(start_date=start_date,end_date=end_date,save=save)


    def required_reserve_ratio_data(self,start_date=None,end_date=None,save=False):
        return self.builder.mdw.get_required_reserve_ratio_data(start_date=start_date,end_date=end_date,save=save)


    def loan_rate(self,start_date=None,end_date=None,save=False):
        return self.builder.mdw.get_loan_rate(start_date=start_date,end_date=end_date,save=save)


    def deposit_rate(self,start_date=None,end_date=None,save=False):
        return self.builder.mdw.get_deposit_rate(start_date=start_date,end_date=end_date,save=save)
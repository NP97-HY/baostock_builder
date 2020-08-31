import baostock as bs
import pandas as pd


class indexs_component(object):
    def __init__(self,):
        lg = bs.login()
        if lg.error_code != 0:
            print("LOGIN FILED:"+lg.error_msg)
        
    
    def get_sz_50_index(self):
        rs = bs.query_sz50_stocks()
        sz50_stocks = []
        while (rs.error_code == '0') and rs.next():
            sz50_stocks.append(rs.get_row_data())
        return sz50_stocks,rs.fields


    def get_stock_industry(self):
        rs = bs.query_stock_industry()
        industry_list = []
        while (rs.error_code == '0') and rs.next():
            industry_list.append(rs.get_row_data())
        return industry_list,rs.fields

    
    def get_hs_300_index(self):
        rs = bs.query_hs300_stocks()
        hs300_stocks = []
        while (rs.error_code == '0') and rs.next():
            hs300_stocks.append(rs.get_row_data())
        return hs300_stocks,rs.fields


    def get_zz_500_index(self):
        rs = bs.query_zz500_stocks()
        zz500_stocks = []
        while (rs.error_code == '0') and rs.next():
            zz500_stocks.append(rs.get_row_data())
        return zz500_stocks,rs.fields


    
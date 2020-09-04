import pandas as pd
from Data_cather import data_catcher as dc
from sqlalchemy import create_engine
from numpy import *


class select_stock(object):
    def __init__(self):
        self.data = dc()
        self.industry = ["食品饮料","交通运输","休闲服务","传媒","公用事业","农林牧渔","化工",
        "医药生物","商业贸易","国防军工","家用电器","建筑材料","房地产","有色金属","机械设备","汽车"
        "电子","电子设备","纺织服装","综合","计算机","轻工制造","通信","采掘","钢铁","银行",
        "非银金融","食品饮料"]

    
    def stock_industry(self):
        engine=create_engine("mysql://root:1qaz!QAZ@localhost:3306/stock?charset=utf8", max_overflow=5)
        sindustry = self.data.stock_industry()
        industry_index = {}
        for a in range(len(self.industry)):
            sindustry = [i for i in sindustry[sindustry["industry"] == self.industry[a]]["code"]]
            trade_data = self.data.trade_data_day(start_date_year = 1,start_date_month = 0,stockcode=sindustry)
            print(self.industry[a])
            price_sum = zeros(450)
            for i in sindustry:
                table_name = i.replace('.','_')
                rd = pd.read_sql('select * from %s;' % table_name,con = engine)
                rd = rd[len(rd)-450:]
                price_sum = price_sum + rd.close
            price_mean = price_mean/len(sindustry)
            industry_index[self.industry[a]] = index
        return industry_index
                
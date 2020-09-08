import pandas as pd
from Data_cather import data_catcher as dc
from sqlalchemy import create_engine
from numpy import *
import matplotlib.pyplot as plt
import matplotlib


class create_index(object):
    def __init__(self):
        self.data = dc()
        self.industry = ["食品饮料","交通运输","休闲服务","传媒","公用事业","农林牧渔","化工",
        "医药生物","商业贸易","国防军工","家用电器","建筑材料","房地产","有色金属","机械设备","汽车",
        "电子","电气设备","纺织服装","综合","计算机","轻工制造","通信","采掘","钢铁","银行",
        "非银金融","食品饮料"]

    
    def stock_industry(self,my_industry:list=None):
        engine=create_engine("mysql://root:1qaz!QAZ@localhost:3306/stock?charset=utf8", max_overflow=5)
        sindustry = self.data.stock_industry()
        if my_industry != None:
            test_industry = my_industry
        else:
            test_industry = self.industry
        idsy = []
        industry_index = {}
        for a in range(len(test_industry)):
            idsy = [i for i in sindustry[sindustry["industry"] == test_industry[a]]["code"]]
            print(test_industry[a])
            price_sum = zeros([len(idsy),480])
            for i in range(len(idsy)):
                table_name = idsy[i].replace('.','_')
                rd = pd.read_sql('select * from %s;' % table_name,con = engine).close
                rd = array(rd)
                mn = mean(rd, axis=0)
                sd = std(rd, axis=0)
                rd = (rd - mn) / sd
                if len(rd)<480:
                    # for r in range(len(rd)):
                    price_sum[i,480-len(rd):] = rd/len(idsy)
                    #price_sum[i,:480] = rd[0]/len(idsy)
                else:
                    # for r in range(480):
                    price_sum[i] += rd[len(rd)-480:]/len(idsy)
            price_mean = sum(price_sum,axis=0)
            industry_index[test_industry[a]] = price_mean
        return industry_index
        

    def stock_mean_index(self,index):
        if index == 'hs_300':
            stocklist = self.data.hs_300_index()
        elif index == 'sz_50':
            stocklist = self.data.sz_50_index()
        elif index == 'zz_500':
            stocklist = self.data.zz_500_index()
        else:
            stocklist = self.data.all_stock_code()
        engine=create_engine("mysql://root:1qaz!QAZ@localhost:3306/stock?charset=utf8", max_overflow=5)
        stocklist = stocklist.code
        price_sum = zeros([len(stocklist),400])
        for i in range(len(stocklist)):
            table_name = stocklist[i].replace('.','_')
            rd = pd.read_sql('select * from %s;' % table_name,con = engine).close
            rd = array(rd)
            mn = mean(rd, axis=0)
            sd = std(rd, axis=0)
            rd = (rd - mn) / sd
            if len(rd)<400:
                price_sum[i,400-len(rd):] = rd/len(stocklist)
            else:
                price_sum[i] += rd[len(rd)-400:]/len(stocklist)
        price_mean = sum(price_sum,axis=0)
        return price_mean


    def plot_mean_index(self,index:list):
        for i in index:
            index_data = self.stock_mean_index(i)
            plt.plot(index_data,label=i)
        plt.legend(loc='upper left')
        plt.grid(True) ##增加格点
        plt.axis('tight') # 坐标轴适应数据量 axis 设置坐标轴
        plt.show()

    
    def plot_industry_index(self,index:list):
        index_data = self.stock_industry(index)
        matplotlib.rcParams['font.sans-serif'] = ['Droid Sans Fallback']
        matplotlib.rcParams['axes.unicode_minus']=False
        for i in index:
            plt.plot(index_data[i],label=u'%s' % i)
        plt.legend(loc='upper left')
        plt.grid(True) ##增加格点
        plt.axis('tight') # 坐标轴适应数据量 axis 设置坐标轴
        plt.show()
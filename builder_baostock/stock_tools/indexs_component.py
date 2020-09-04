import pandas as pd


class indexs_component(object):
    def __init__(self,bs,ENGINE):
        self.bs = bs


    def _tosql(self,targetStock):
            ymd = []
            try:
                rd = pd.read_sql('select * from %s;' % targetStock,con = self.engine)
                ymd = rd.date[len(rd)-1].split("-")
                next_date = datetime.date(int(ymd[0]),int(ymd[1]),int(ymd[2]))
                tar_date = (datetime.date.today()-next_date).days-1
                if len(result)-tar_date<0:
                    raise Exception("数据长度不足")
            except sqlalchemy.exc.ProgrammingError as e:
                tar_date = 0
            filtration_data = result[tar_date:]
            try:
                filtration_data.to_sql(name=targetStock,con=self.engine,
                                    if_exists='append',index=False)
            except Exception as e:
                print(targetStock+"保存数据失败")
            stock_data_list[targetStock] = result
            result = pd.DataFrame(result, columns=rs.fields)
            print(targetStock+"  finish")
            
 
    def get_sz_50_index(self,save=True):
        rs = self.bs.query_sz50_stocks()
        sz50_stocks = []
        while (rs.error_code == '0') and rs.next():
            sz50_stocks.append(rs.get_row_data())
        result = pd.DataFrame(sz50_stocks, columns=rs.fields)
        if save == True:
            result.to_csv("data_home/sz_50.csv", encoding="gbk", index=False)
        return result


    def get_stock_industry(self,save=True):
        rs = self.bs.query_stock_industry()
        industry_list = []
        while (rs.error_code == '0') and rs.next():
            industry_list.append(rs.get_row_data())
        result = pd.DataFrame(industry_list, columns=rs.fields)
        if save == True:
            result.to_csv("data_home/stock_industry.csv", encoding="gbk", index=False)
        return result

    
    def get_hs_300_index(self,save=True):
        rs = self.bs.query_hs300_stocks()
        hs300_stocks = []
        while (rs.error_code == '0') and rs.next():
            hs300_stocks.append(rs.get_row_data())
        result = pd.DataFrame(hs300_stocks, columns=rs.fields)
        if save == True:
            result.to_csv("data_home/hs_300.csv", encoding="gbk", index=False)
        return result


    def get_zz_500_index(self,save=True):
        rs = self.bs.query_zz500_stocks()
        zz500_stocks = []
        while (rs.error_code == '0') and rs.next():
            zz500_stocks.append(rs.get_row_data())
        result = pd.DataFrame(zz500_stocks, columns=rs.fields)
        if save == True:
            result.to_csv("data_home/zz_500.csv", encoding="gbk", index=False)
        return result


    
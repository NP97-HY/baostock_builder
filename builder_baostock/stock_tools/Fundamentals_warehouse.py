import pandas as pd


class fundamentals_warehouse(object):
    def __init__(self,bs):
        self.bs = bs
        self.my_time = time.localtime(time.time())
        self.year = self.my_time.tm_year

    
    def get_profit_data(self,codelist:list=None,year:int=5,quarter:int=None,save=True):
        """
        code 	证券代码 	
        pubDate 	公司发布财报的日期 	
        statDate 	财报统计的季度的最后一天, 比如2017-03-31, 2017-06-30 	
        roeAvg 	净资产收益率(平均)(%) 	归属母公司股东净利润/[(期初归属母公司股东的权益+期末归属母公司股东的权益)/2]*100%
        npMargin 	销售净利率(%) 	净利润/营业收入*100%
        gpMargin 	销售毛利率(%) 	毛利/营业收入*100%=(营业收入-营业成本)/营业收入*100%
        netProfit 	净利润(元) 	
        epsTTM 	每股收益 	归属母公司股东的净利润TTM/最新总股本
        MBRevenue 	主营营业收入(元) 	
        totalShare 	总股本 	
        liqaShare 	流通股本 	
        """
        data_saver = []
        for stockcode in codelist:
            for r in range(0,i+1)
                for qua in range(1,5)
                    rs = self.bs.query_profit_data(code=stockcode,year=self.year-r,quarter=qua)
                    while(rs.error_code == "0") and rs.next():
                        data_saver.append(rs.get_row_data())
        data_saver = [i for i in data_saver if i != '']
        if save == True:
            result = pd.DataFrame(data_saver, columns=rs.fields)
            result.to_csv("data_home/profit_data.csv", encoding="gbk", index=False)
        return result


    def get_money_supply_data(self,codelist:list=None,year:int=5,quarter:int=None,save=True):
        """
        code 	证券代码 	
        pubDate 	公司发布财报的日期 	
        statDate 	财报统计的季度的最后一天, 比如2017-03-31, 2017-06-30 	
        NRTurnRatio 	应收账款周转率(次) 	营业收入/[(期初应收票据及应收账款净额+期末应收票据及应收账款净额)/2]
        NRTurnDays 	应收账款周转天数(天) 	季报天数/应收账款周转率(一季报：90天，中报：180天，三季报：270天，年报：360天)
        INVTurnRatio 	存货周转率(次) 	营业成本/[(期初存货净额+期末存货净额)/2]
        INVTurnDays 	存货周转天数(天) 	季报天数/存货周转率(一季报：90天，中报：180天，三季报：270天，年报：360天)
        CATurnRatio 	流动资产周转率(次) 	营业总收入/[(期初流动资产+期末流动资产)/2]
        AssetTurnRatio 	总资产周转率 	营业总收入/[(期初资产总额+期末资产总额)/2] 	
        """
        data_saver = []
        for stockcode in codelist:
            for r in range(0,i+1)
                for qua in range(1,5)
                    rs = self.bs.query_operation_data(code=stockcode,year=self.year-r,quarter=qua)
                    while(rs.error_code == "0") and rs.next():
                        data_saver.append(rs.get_row_data())
        data_saver = [i for i in data_saver if i != '']
        if save == True:
            result = pd.DataFrame(data_saver, columns=rs.fields)
            result.to_csv("data_home/money_supply_data.csv", encoding="gbk", index=False)
        return result

import pandas as pd
import time


class fundamentals_warehouse(object):
    def __init__(self,bs,ENGINE):
        self.bs = bs
        self.engine = ENGINE
        self.my_time = time.localtime(time.time())
        self.year = self.my_time.tm_year


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

    
    def get_profit_data(self,codelist=None,year:int=5,quarter:int=None,save=True):
        """
        季频盈利能力
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
            for r in range(0,year+1):
                for qua in range(1,5):
                    rs = self.bs.query_profit_data(code=stockcode,year=self.year-r,quarter=qua)
                    while(rs.error_code == "0") and rs.next():
                        data_saver.append(rs.get_row_data())
        data_saver = [i for i in data_saver if i != '']
        result = pd.DataFrame(data_saver, columns=rs.fields)
        if save == True:
            result.to_csv("data_home/profit_data.csv", encoding="gbk", index=False)
        return result


    def get_operation_data(self,codelist=None,year:int=5,quarter:int=None,save=True):
        """
        季频营运能力
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
            for r in range(0,year+1):
                for qua in range(1,5):
                    rs = self.bs.query_operation_data(code=stockcode,year=self.year-r,quarter=qua)
                    while(rs.error_code == "0") and rs.next():
                        data_saver.append(rs.get_row_data())
        data_saver = [i for i in data_saver if i != '']
        result = pd.DataFrame(data_saver, columns=rs.fields)
        if save == True:
            result.to_csv("data_home/operation_data.csv", encoding="gbk", index=False)
        return result


    def get_growth_data(self,codelist=None,year:int=5,quarter:int=None,save=True):
        """
        季频成长能力
        code 	证券代码 	
        pubDate 	公司发布财报的日期 	
        statDate 	财报统计的季度的最后一天, 比如2017-03-31, 2017-06-30 	
        YOYEquity 	净资产同比增长率 	(本期净资产-上年同期净资产)/上年同期净资产的绝对值*100%
        YOYAsset 	总资产同比增长率 	(本期总资产-上年同期总资产)/上年同期总资产的绝对值*100%
        YOYNI 	净利润同比增长率 	(本期净利润-上年同期净利润)/上年同期净利润的绝对值*100%
        YOYEPSBasic 	基本每股收益同比增长率 	(本期基本每股收益-上年同期基本每股收益)/上年同期基本每股收益的绝对值*100%
        YOYPNI 	归属母公司股东净利润同比增长率 	(本期归属母公司股东净利润-上年同期归属母公司股东净利润)/上年同期归属母公司股东净利润的绝对值*100% 
        """
        data_saver = []
        for stockcode in codelist:
            for r in range(0,year+1):
                for qua in range(1,5):
                    rs = self.bs.query_growth_data(code=stockcode,year=self.year-r,quarter=qua)
                    while(rs.error_code == "0") and rs.next():
                        data_saver.append(rs.get_row_data())
        data_saver = [i for i in data_saver if i != '']
        result = pd.DataFrame(data_saver, columns=rs.fields)
        if save == True:
            result.to_csv("data_home/growth_data.csv", encoding="gbk", index=False)
        return result


    def get_balance_data(self,codelist=None,year:int=5,quarter:int=None,save=True):
        """
        季频偿债能力
        code 	证券代码 	
        pubDate 	公司发布财报的日期 	
        statDate 	财报统计的季度的最后一天, 比如2017-03-31, 2017-06-30 	
        currentRatio 	流动比率 	流动资产/流动负债
        quickRatio 	速动比率 	(流动资产-存货净额)/流动负债
        cashRatio 	现金比率 	(货币资金+交易性金融资产)/流动负债
        YOYLiability 	总负债同比增长率 	(本期总负债-上年同期总负债)/上年同期中负债的绝对值*100%
        liabilityToAsset 	资产负债率 	负债总额/资产总额
        assetToEquity 	权益乘数 	资产总额/股东权益总额=1/(1-资产负债率) 
        """
        data_saver = []
        for stockcode in codelist:
            for r in range(0,year+1):
                for qua in range(1,5):
                    rs = self.bs.query_balance_data(code=stockcode,year=self.year-r,quarter=qua)
                    while(rs.error_code == "0") and rs.next():
                        data_saver.append(rs.get_row_data())
        data_saver = [i for i in data_saver if i != '']
        result = pd.DataFrame(data_saver, columns=rs.fields)
        if save == True:
            result.to_csv("data_home/balance_data.csv", encoding="gbk", index=False)
        return result 


    def get_cash_flow_data(self,codelist=None,year:int=5,quarter:int=None,save=True):
        """
        季频现金流量
        code 	证券代码 	
        pubDate 	公司发布财报的日期 	
        statDate 	财报统计的季度的最后一天, 比如2017-03-31, 2017-06-30 	
        CAToAsset 	流动资产除以总资产 	
        NCAToAsset 	非流动资产除以总资产 	
        tangibleAssetToAsset 	有形资产除以总资产 	
        ebitToInterest 	已获利息倍数 	息税前利润/利息费用
        CFOToOR 	经营活动产生的现金流量净额除以营业收入 	
        CFOToNP 	经营性现金净流量除以净利润 	
        CFOToGr 	经营性现金净流量除以营业总收入 
        """
        data_saver = []
        for stockcode in codelist:
            for r in range(0,year+1):
                for qua in range(1,5):
                    rs = self.bs.query_cash_flow_data(code=stockcode,year=self.year-r,quarter=qua)
                    while(rs.error_code == "0") and rs.next():
                        data_saver.append(rs.get_row_data())
        data_saver = [i for i in data_saver if i != '']
        result = pd.DataFrame(data_saver, columns=rs.fields)
        if save == True:
            result.to_csv("data_home/cash_flow_data.csv", encoding="gbk", index=False)
        return result 


    def get_dupont_data(self,codelist=None,year:int=5,quarter:int=None,save=True):
        """
        季频杜邦指数
        code 	证券代码 	
        pubDate 	公司发布财报的日期 	
        statDate 	财报统计的季度的最后一天, 比如2017-03-31, 2017-06-30 	
        dupontROE 	净资产收益率 	归属母公司股东净利润/[(期初归属母公司股东的权益+期末归属母公司股东的权益)/2]*100%
        dupontAssetStoEquity 	权益乘数，反映企业财务杠杆效应强弱和财务风险 	平均总资产/平均归属于母公司的股东权益
        dupontAssetTurn 	总资产周转率，反映企业资产管理效率的指标 	营业总收入/[(期初资产总额+期末资产总额)/2]
        dupontPnitoni 	归属母公司股东的净利润/净利润，反映母公司控股子公司百分比。如果企业追加投资，扩大持股比例，则本指标会增加。 	
        dupontNitogr 	净利润/营业总收入，反映企业销售获利率 	
        dupontTaxBurden 	净利润/利润总额，反映企业税负水平，该比值高则税负较低。净利润/利润总额=1-所得税/利润总额 	
        dupontIntburden 	利润总额/息税前利润，反映企业利息负担，该比值高则税负较低。利润总额/息税前利润=1-利息费用/息税前利润
        dupontEbittogr 	息税前利润/营业总收入，反映企业经营利润率，是企业经营获得的可供全体投资人（股东和债权人）分配的盈利占企业全部营收收入的百分比 	
        """
        data_saver = []
        for stockcode in codelist:
            for r in range(0,year+1):
                for qua in range(1,5):
                    rs = self.bs.query_dupont_data(code=stockcode,year=self.year-r,quarter=qua)
                    while(rs.error_code == "0") and rs.next():
                        data_saver.append(rs.get_row_data())
        data_saver = [i for i in data_saver if i != '']
        result = pd.DataFrame(data_saver, columns=rs.fields)
        if save == True:
            result.to_csv("data_home/dupont_data.csv", encoding="gbk", index=False)
        return result 


    def get_performance_express_report(self,codelist=None,year:int=5,quarter:int=None,save=True):
        """
        季频业绩快报
        code 	证券代码
        performanceExpPubDate 	业绩快报披露日
        performanceExpStatDate 	业绩快报统计日期
        performanceExpUpdateDate 	业绩快报披露日(最新)
        performanceExpressTotalAsset 	业绩快报总资产
        performanceExpressNetAsset 	业绩快报净资产
        performanceExpressEPSChgPct 	业绩每股收益增长率
        performanceExpressROEWa 	业绩快报净资产收益率ROE-加权
        performanceExpressEPSDiluted 	业绩快报每股收益EPS-摊薄
        performanceExpressGRYOY 	业绩快报营业总收入同比
        performanceExpressOPYOY 	业绩快报营业利润同比 	
        """
        data_saver = []
        for stockcode in codelist:
            for r in range(0,year+1):
                for qua in range(1,5):
                    rs = self.bs.query_performance_express_report(code=stockcode,year=self.year-r,quarter=qua)
                    while(rs.error_code == "0") and rs.next():
                        data_saver.append(rs.get_row_data())
        data_saver = [i for i in data_saver if i != '']
        result = pd.DataFrame(data_saver, columns=rs.fields)
        if save == True:
            result.to_csv("data_home/performance_express_report.csv", encoding="gbk", index=False)
        return result 


    def get_forcast_report(self,codelist=None,save=True):
        """
        季频业绩预告
        code 	证券代码
        profitForcastExpPubDate 	业绩预告发布日期
        profitForcastExpStatDate 	业绩预告统计日期
        profitForcastType 	业绩预告类型
        profitForcastAbstract 	业绩预告摘要
        profitForcastChgPctUp 	预告归属于母公司的净利润增长上限(%)
        profitForcastChgPctDwn 	预告归属于母公司的净利润增长下限(%)	
        """
        data_saver = []
        for stockcode in codelist:
            rs = self.bs.query_forcast_report(code=stockcode)
            while(rs.error_code == "0") and rs.next():
                data_saver.append(rs.get_row_data())
        result = pd.DataFrame(data_saver, columns=rs.fields)
        if save == True:
            result.to_csv("data_home/forcast_report.csv", encoding="gbk", index=False)
        return result 
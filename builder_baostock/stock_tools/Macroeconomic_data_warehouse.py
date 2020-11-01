import pandas as pd
import time


class macroeconomic_Data_Warehouse(object):
    def __init__(self,bs,years:int=3,end_date:str=None):
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
        参数名称 	参数描述
        pubDate 	发布日期
        demandDepositRate 	活期存款(不定期)
        fixedDepositRate3Month 	定期存款(三个月)
        fixedDepositRate6Month 	定期存款(半年)
        fixedDepositRate1Year 	定期存款整存整取(一年)
        fixedDepositRate2Year 	定期存款整存整取(二年)
        fixedDepositRate3Year 	定期存款整存整取(三年)
        fixedDepositRate5Year 	定期存款整存整取(五年)
        installmentFixedDepositRate1Year 	零存整取、整存零取、存本取息定期存款(一年)
        installmentFixedDepositRate3Year 	零存整取、整存零取、存本取息定期存款(三年)
        installmentFixedDepositRate5Year 	零存整取、整存零取、存本取息定期存款(五年)
        """
        if end_date == None:
            end_date = self.end_date
        if start_date == None:
            start_date = self.start_date
        data_saver = []
        rs = self.bs.query_deposit_rate_data(start_date,end_date)
        while(rs.error_code == "0") and rs.next():
            data_saver.append(rs.get_row_data())
        result = pd.DataFrame(data_saver, columns=rs.fields)
        if save == True:
            result.to_csv("data_home/deposit_rate.csv")
        return result

    def get_loan_rate(self,start_date=None,end_date:str=None,save=True):
        """
        贷款利率
        参数名称 	参数描述
        pubDate 	发布日期
        loanRate6Month 	6个月贷款利率
        loanRate6MonthTo1Year 	6个月至1年贷款利率
        loanRate1YearTo3Year 	1年至3年贷款利率
        loanRate3YearTo5Year 	3年至5年贷款利率
        loanRateAbove5Year 	5年以上贷款利率
        mortgateRateBelow5Year 	5年以下住房公积金贷款利率
        mortgateRateAbove5Year 	5年以上住房公积金贷款利率 
        """
        if end_date == None:
            end_date = self.end_date
        if start_date == None:
            start_date = self.start_date
        data_saver = []
        rs = self.bs.query_loan_rate_data(start_date,end_date)
        while(rs.error_code == "0") and rs.next():
            data_saver.append(rs.get_row_data())
        result = pd.DataFrame(data_saver, columns=rs.fields)
        if save == True:
            result.to_csv("data_home/loan_rate.csv")
        return result

    def get_required_reserve_ratio_data(self,start_date=None,end_date=None,yearType="0",save=True):
        """
        存款准备金率
        参数名称 	参数描述
        pubDate 	公告日期
        effectiveDate 	生效日期
        bigInstitutionsRatioPre 	人民币存款准备金率：大型存款类金融机构 调整前
        bigInstitutionsRatioAfter 	人民币存款准备金率：大型存款类金融机构 调整后
        mediumInstitutionsRatioPre 	人民币存款准备金率：中小型存款类金融机构 调整前
        mediumInstitutionsRatioAfter 	人民币存款准备金率：中小型存款类金融机构 调整后 
        """
        if end_date == None:
            end_date = self.end_date
        if start_date == None:
            start_date = self.start_date
        data_saver = []
        rs = self.bs.query_required_reserve_ratio_data(start_date,end_date,yearType)
        while(rs.error_code == "0") and rs.next():
            data_saver.append(rs.get_row_data())
        result = pd.DataFrame(data_saver, columns=rs.fields)
        if save == True:
            result.to_csv("data_home/required_reserve_ratio_data.csv", encoding="gbk", index=False)
        return result

    def get_money_supply_data_month(self,start_date=None,end_date=None,save=True):
        """
        货币供应量
        参数名称 	参数描述
        statYear 	统计年度
        statMonth 	统计月份
        m0Month 	货币供应量M0（月）
        m0YOY 	货币供应量M0（同比）
        m0ChainRelative 	货币供应量M0（环比）
        m1Month 	货币供应量M1（月）
        m1YOY 	货币供应量M1（同比）
        m1ChainRelative 	货币供应量M1（环比）
        m2Month 	货币供应量M2（月）
        m2YOY 	货币供应量M2（同比）
        m2ChainRelative 	货币供应量M2（环比）
        """
        if end_date == None:
            end_date = self.end_date
        if start_date == None:
            start_date = self.start_date
        data_saver = []
        rs = self.bs.query_money_supply_data_month(start_date,end_date)
        while(rs.error_code == "0") and rs.next():
            data_saver.append(rs.get_row_data())
        result = pd.DataFrame(data_saver, columns=rs.fields)
        if save == True:
            result.to_csv("data_home/money_supply_data.csv", encoding="gbk", index=False)
        return result

    def get_money_supply_data_year(self,start_date=None,end_date=None,save=True):
        """
        货币供应量(年底余额)
        参数名称 	参数描述
        statYear 	统计年度
        m0Year 	年货币供应量M0（亿元）
        m0YearYOY 	年货币供应量M0（同比）
        m1Year 	年货币供应量M1（亿元）
        m1YearYOY 	年货币供应量M1（同比）
        m2Year 	年货币供应量M2（亿元）
        m2YearYOY 	年货币供应量M2（同比）
        """
        if end_date == None:
            end_date = self.end_date
        if start_date == None:
            start_date = self.start_date
        data_saver = []
        rs = self.bs.query_money_supply_data_year(str(self.my_time.tm_year-10),str(self.my_time.tm_year))
        while(rs.error_code == "0") and rs.next():
            data_saver.append(rs.get_row_data())
        result = pd.DataFrame(data_saver, columns=rs.fields)
        if save == True:
            result.to_csv("data_home/money_supply_data.csv", encoding="gbk", index=False)
        return result

    def get_shibor_data(self,start_date=None,end_date=None,save=True):
        """
        银行间同业拆放利率
        参数名称 	参数描述
        date 	日期
        shiborON 	隔夜拆借利率
        shibor1W 	1周拆放利率
        shibor2W 	2周拆放利率
        shibor1M 	1个月拆放利率
        shibor3M 	3个月拆放利率
        shibor6M 	6个月拆放利率
        shibor9M 	9个月拆放利率
        shibor1Y 	1年拆放利率 
        """
        if end_date == None:
            end_date = self.end_date
        if start_date == None:
            start_date = self.start_date
        data_saver = []
        rs = self.bs.query_shibor_data(start_date,end_date)
        while(rs.error_code == "0") and rs.next():
            data_saver.append(rs.get_row_data())
        result = pd.DataFrame(data_saver, columns=rs.fields)
        if save == True:
            result.to_csv("data_home/shibor_data.csv", encoding="gbk", index=False)
        return result
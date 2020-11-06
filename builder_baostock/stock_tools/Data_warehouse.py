import pandas as pd
import time



class data_Warehouse(object):
    """
    get stock daily data
    """
    def __init__(self,bs):
        self.bs = bs


    def get_data(self,date="1",start_date:str=None,start_date_year:int=2,start_date_month:int=0,
                frequency="d",adjustflag="2",stocklist=None,save=False):
        if date == "1":
            self.my_time = time.localtime(time.time())
            month = str(self.my_time.tm_mon-start_date_month).zfill(2)
            day = str(self.my_time.tm_mday).zfill(2)
            self.date = "{}-{}-{}".format(self.my_time.tm_year,month,day)
        else:
            self.date = date
        if start_date == None:
            self.start_date = "{}-{}-{}".format(self.my_time.tm_year-start_date_year,month,day)
        else:
            self.start_date = start_date
        self.frequency = frequency
        self.adjustflag = adjustflag
        return self._query_history_data_list(stocklist=stocklist,save=save)


    def _query_history_data_list(self,stocklist,save):
        """
        get history data for a group stocks
        targetStock : your aim stock,like: ["sh.600300","sh.601987"]

        date 	交易所行情日期 	
        code 	证券代码 	
        open 	开盘价 	
        high 	最高价 	
        low 	最低价 	
        close 	收盘价 	
        preclose 	前收盘价 	见表格下方详细说明
        volume 	成交量（累计 单位：股） 	
        amount 	成交额（单位：人民币元） 	
        adjustflag 	复权状态(1：后复权， 2：前复权，3：不复权） 	
        turn 	换手率 	[指定交易日的成交量(股)/指定交易日的股票的流通股总股数(股)]*100%
        tradestatus 	交易状态(1：正常交易 0：停牌） 	
        pctChg 	涨跌幅（百分比） 	日涨跌幅=[(指定交易日的收盘价-指定交易日前收盘价)/指定交易日前收盘价]*100%
        isST 	是否ST股，1是，0否 	
        """
        stock_data_list = {}
        for targetStock in stocklist:
            # if tosql:
            #     try:
            #         rd = pd.read_sql('select * from %s;' % targetStock,con = self.engine)
            #     except sqlalchemy.exc.ProgrammingError as e:
            rs = self.bs.query_history_k_data_plus(targetStock,
                                        "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST",
                                        start_date=self.start_date, end_date=self.date,
                                        frequency=self.frequency, adjustflag=self.adjustflag)
            if rs.error_code != "0":
                print("GET {} DATA FAILED:".format(targetStock)+rs.error_msg)
                return 0
            datalist = []
            while (rs.error_code == "0") & rs.next():
                datalist.append(rs.get_row_data())
            result = pd.DataFrame(datalist, columns=rs.fields)
            result = result[result["tradestatus"]=="1"]
            result['open'] = result['open'].astype(float)
            result['high'] = result['high'].astype(float)
            result['low'] = result['low'].astype(float)
            result['close'] = result['close'].astype(float)
            result['preclose'] = result['preclose'].astype(float)
            try:
                result['volume'] = result['volume'].astype(float)
            except Exception as e:
                return False
            result['amount'] = result['amount'].astype(float)
            result['turn'] = result['turn'].astype(float)
            result['pctChg'] = result['pctChg'].astype(float)
            stock_data_list[targetStock] = result
            result = pd.DataFrame(result, columns=rs.fields)
            print(targetStock+"  finish")
            if save == True:
                result.to_csv("data_home/%s.csv" % targetStock, encoding="gbk", index=False)
        return stock_data_list

    
    #def 
            
    
        
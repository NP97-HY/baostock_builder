import builder_baostock as bb


fw=bb.get_tools('fw')


@fw.all_data
def profit_data(self,stockname:str=None,year:int=None,qua:int=None):
    print(stockname)
    print(year)
    print(qua)
    rs = fw.bs.query_profit_data(code=stockname,year=year,quarter=qua)
    return rs
    
@fw.all_data
def operation_data(self,stockname=None,year:int=None,qua:int=None):
    rs = fw.bs.query_operation_data(code=stockname,year=year,quarter=qua)
    return rs

@fw.all_data
def growth_data(self,stockname=None,year:int=None,qua:int=None):
    rs = fw.bs.query_growth_data(code=stockname,year=year,quarter=qua)
    return rs

@fw.all_data
def balance_data(self,stockname=None,year:int=None,qua:int=None):
    rs = fw.bs.query_balance_data(code=stockname,year=year,quarter=qua)
    return rs

@fw.all_data
def cash_flow_data(self,stockname=None,year:int=None,qua:int=None):
    rs = fw.bs.query_cash_flow_data(code=stockname,year=year,quarter=qua)
    return rs

@fw.all_data
def dupont_data(self,stockname=None,year:int=None,qua:int=None):
    rs = fw.bs.query_dupont_data(code=stockname,year=year,quarter=qua)
    return rs

# @fundamentals_warehouse.all_data
# def performance_express_report(self,stockname=None,year:int=None,qua:int=None):
#     rs = bs.query_performance_express_report(code=stockname,year=year,quarter=qua)
#     return rs
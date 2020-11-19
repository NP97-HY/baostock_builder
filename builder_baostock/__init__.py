from .stock_tools.stock_catcher import stock_catcher
from .stock_tools.Macroeconomic_data_warehouse import macroeconomic_Data_Warehouse
from .stock_tools.indexs_component import indexs_component
from .stock_tools.Data_warehouse import data_Warehouse
from .stock_tools.Fundamentals_warehouse import fundamentals_warehouse
import baostock as bs


class tools(object):
    def __init__(self):
        lg = bs.login()
        if lg.error_code != "0":
            print("LOGIN FAILED:"+lg.error_msg)
        self.sc = stock_catcher(bs)
        self.mdw = macroeconomic_Data_Warehouse(bs)
        self.ic = indexs_component(bs)
        self.dw = data_Warehouse(bs)
        self.fw = fundamentals_warehouse(bs)


def get_tools(ty=None):
    if ty == "sc":
        lg = bs.login()
        if lg.error_code != "0":
            print("LOGIN FAILED:"+lg.error_msg)
        return stock_catcher(bs)
    if ty == "mdw":
        lg = bs.login()
        if lg.error_code != "0":
            print("LOGIN FAILED:"+lg.error_msg)
        return macroeconomic_Data_Warehouse(bs)
    if ty == "ic":
        lg = bs.login()
        if lg.error_code != "0":
            print("LOGIN FAILED:"+lg.error_msg)
        return indexs_component(bs)
    if ty == "dw":
        lg = bs.login()
        if lg.error_code != "0":
            print("LOGIN FAILED:"+lg.error_msg)
        return data_Warehouse(bs)
    if ty == "fw":
        lg = bs.login()
        if lg.error_code != "0":
            print("LOGIN FAILED:"+lg.error_msg)
        return fundamentals_warehouse(bs)
    if ty == None:
        return tools()



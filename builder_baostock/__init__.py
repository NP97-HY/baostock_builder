from .stock_tools.stock_catcher import stock_catcher
from .stock_tools.Macroeconomic_data_warehouse import macroeconomic_Data_Warehouse
from .stock_tools.indexs_component import indexs_component
from .stock_tools.Data_warehouse import data_Warehouse
from .stock_tools.Fundamentals_warehouse import fundamentals_warehouse
import baostock as bs
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

class tools(object):
    def __init__(self):
        lg = bs.login()
        if lg.error_code != "0":
            print("LOGIN FAILED:"+lg.error_msg)
        ENGINE=create_engine("mysql://root:1qaz!QAZ@localhost:3306/test1?charset=utf8", max_overflow=5)
        self.sc = stock_catcher(bs,ENGINE)
        self.mdw = macroeconomic_Data_Warehouse(bs,ENGINE)
        self.ic = indexs_component(bs,ENGINE)
        self.dw = data_Warehouse(bs,ENGINE)
        self.fw = fundamentals_warehouse(bs,ENGINE)


def get_tools(ty=None):
    if ty == "sc":
        return stock_catcher(bs,create_engine("mysql://root:1qaz!QAZ@localhost:3306/stock?charset=utf8", max_overflow=5))
    if ty == "mdw":
        return macroeconomic_Data_Warehouse(bs,create_engine("mysql://root:1qaz!QAZ@localhost:3306/stock?charset=utf8", max_overflow=5))
    if ty == "ic":
        return indexs_component(bs,create_engine("mysql://root:1qaz!QAZ@localhost:3306/stock?charset=utf8", max_overflow=5))
    if ty == "dw":
        return data_Warehouse(bs,create_engine("mysql://root:1qaz!QAZ@localhost:3306/stock?charset=utf8", max_overflow=5))
    if ty == "fw":
        return fundamentals_warehouse(bs,create_engine("mysql://root:1qaz!QAZ@localhost:3306/stock?charset=utf8", max_overflow=5))
    if ty == None:
        return tools()

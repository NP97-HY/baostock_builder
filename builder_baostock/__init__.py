from stock_tools.stock_catcher import stock_catcher
from stock_tools.Macroeconomic_data_warehouse import macroeconomic_Data_Warehouse
from stock_tools.indexs_component import indexs_component
from stock_tools.Data_warehouse import data_Warehouse

class tools(object):
    def __init__(self):
        self.sc = stock_catcher()
        self.mdw = macroeconomic_Data_Warehouse()
        self.ic = indexs_component()
        self.dw = data_Warehouse()


def get_tools(ty):
    if ty == "sc":
        return stock_catcher()
    if ty == "mdw":
        return macroeconomic_Data_Warehouse()
    if ty == "ic":
        return indexs_component()
    if ty == "dw":
        return data_Warehouse()
    if ty == None:
        return tools()

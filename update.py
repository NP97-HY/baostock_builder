from database_updata import database_updata as du
from talib_builder import talib_builder as tb
from multiprocessing import Process
import builder_baostock as bb

def _db_updata(stock_list):
    dbu = du(stock_list.code,stock_list.type)
    dbu.updata_db_all()


def _tb_updata(stock_list):
    tbu = tb(stock_list.code,stock_list.type)
    tbu.MACD_build()


def updata_stock():
    my_crl = bb.get_tools()
    code_pool = my_crl.sc.get_all_code()
    _db_updata(code_pool)


def updata_macd():
    my_crl = bb.get_tools()
    code_pool = my_crl.sc.get_all_code()
    _tb_updata(code_pool)
    

class Run(Process):
    def __init__(self,stock_list):
        super().__init__()
        self.stock_list=stock_list


    def run(self):
        # print("start db updata")
        # _db_updata(self.stock_list)
        print("finish stock db  updata")
        _tb_updata(self.stock_list)
        print("finish MACD db  updata")

if __name__ == "__main__":
    my_crl = bb.get_tools()
    code_pool = my_crl.sc.get_all_code()
    half = len(code_pool)/2
    stocklist_1 = code_pool[:int(half/2)]
    stocklist_2 = code_pool[int(half/2)+1:int(half)]
    stocklist_3 = code_pool[int(half)+1:int(half*3/2)]
    stocklist_4 = code_pool[int(half*3/2):]
    p1 = Run(stocklist_1)
    p2 = Run(stocklist_2)
    p3 = Run(stocklist_3)
    p4 = Run(stocklist_4)
    p1.start()
    p2.start()
    p3.start()
    p4.start()
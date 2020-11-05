from database_updata import database_updata as du
from talib_builder import talib_builder as tb
from multiprocessing import Process
import builder_baostock as bb

def db_updata(stock_list:list):
    dbu = du(stock_list)
    dbu.updata_db_all()


def tb_updata(stock_list:list):
    tbu = tb(stock_list)
    tbu.MACD_build()


class Run(Process):
    def __init__(self,stock_list:list):
        super().__init__()
        self.stock_list=stock_list


    def run(self):
        print("start db updata")
        db_updata(self.stock_list)
        print("finish stock db  updata")
        tb_updata(self.stock_list)
        print("finish MACD db  updata")

if __name__ == "__main__":
    my_crl = bb.get_tools()
    code_pool = my_crl.sc.get_all_code().code
    half = len(code_pool)/2
    stocklist_1 = code_pool[:int(half)]
    stocklist_2 = code_pool[int(half)+1:]
    p1 = Run(stocklist_1)
    p2 = Run(stocklist_2)
    p1.start()
    p2.start()
from database_manager.database_updata import database_updata as du
from database_manager.talib_builder import talib_builder as tb
from multiprocessing import Process
import builder_baostock as bb

def _db_updata(stock_list):
    dbu = du(stock_list)
    dbu.updata_FW_profit()



def updata_stock():
    my_crl = bb.get_tools()
    code_pool = my_crl.sc.get_all_code()
    _db_updata(code_pool)


    

class Run(Process):
    def __init__(self,stock_list):
        super().__init__()
        self.stock_list=stock_list


    def run(self):
        print("start db updata")
        _db_updata(self.stock_list)
        print("finish stock db  updata")

if __name__ == "__main__":
    my_crl = bb.get_tools()
    code_pool = my_crl.sc.get_all_code()
    code_pool = code_pool[220:len(code_pool)-350]
    half = len(code_pool)/2
    stocklist_1 = code_pool[:int(half/4)]
    stocklist_2 = code_pool[int(half/4)+1:int(half/2)]
    stocklist_3 = code_pool[int(half/2)+1:int(half/4*3)]
    stocklist_4 = code_pool[int(half/4*3)+1:int(half)]
    stocklist_5 = code_pool[int(half)+1:int(half/4*5)]
    stocklist_6 = code_pool[int(half/4*5)+1:int(half/4*6)]
    stocklist_7 = code_pool[int(half/4*6)+1:int(half/4*7)]
    stocklist_8 = code_pool[int(half/4*7)+1:]
    p1 = Run(stocklist_1)
    p2 = Run(stocklist_2)
    p3 = Run(stocklist_3)
    p4 = Run(stocklist_4)
    p5 = Run(stocklist_5)
    p6 = Run(stocklist_6)
    p7 = Run(stocklist_7)
    p8 = Run(stocklist_8)
    p1.start()
    p2.start()
    p3.start()
    p4.start()
    p5.start()
    p6.start()
    p7.start()
    p8.start()
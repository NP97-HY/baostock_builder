from sqlalchemy import create_engine
import pymysql


class DB_HOME(object):
    def __init__(self):
        pymysql.install_as_MySQLdb()
        
    def DB_stock_build(self):
        return create_engine("mysql://root:1qaz!QAZ@localhost:3306/stock_data?charset=utf8", max_overflow=5)

    def DB_MACD_build(self):
        return create_engine("mysql://root:1qaz!QAZ@localhost:3306/stock_MACD?charset=utf8", max_overflow=5)

    def DB_stockindex_build(self):
        return create_engine("mysql://root:1qaz!QAZ@localhost:3306/stock_index?charset=utf8", max_overflow=5)

    def DB_MA_build(self):
        return create_engine("mysql://root:1qaz!QAZ@localhost:3306/stock_MA?charset=utf8", max_overflow=5)

    def DB_FP_build(self):
        return create_engine("mysql://root:1qaz!QAZ@localhost:3306/FW_profit?charset=utf8", max_overflow=5)

    def DB_SL_build(self):
        return create_engine("mysql://root:1qaz!QAZ@localhost:3306/stocklist?charset=utf8", max_overflow=5)

    def DB_SIL_build(self):
        return create_engine("mysql://root:1qaz!QAZ@localhost:3306/stockindexlist?charset=utf8", max_overflow=5)

    def DB_FO_build(self):
        return create_engine("mysql://root:1qaz!QAZ@localhost:3306/FW_operation?charset=utf8", max_overflow=5)

    def DB_FG_build(self):
        return create_engine("mysql://root:1qaz!QAZ@localhost:3306/Fw_growth?charset=utf8", max_overflow=5)

    def DB_FB_build(self):
        return create_engine("mysql://root:1qaz!QAZ@localhost:3306/Fw_balance?charset=utf8", max_overflow=5)

    def DB_FCF_build(self):
        return create_engine("mysql://root:1qaz!QAZ@localhost:3306/Fw_cash_flow?charset=utf8", max_overflow=5)

    def DB_FD_build(self):
        return create_engine("mysql://root:1qaz!QAZ@localhost:3306/Fw_dupont?charset=utf8", max_overflow=5)


DH=DB_HOME()
DB_stock=DH.DB_stock_build()    #股票日线数据
DB_MACD=DH.DB_MACD_build()      #MACD数据
DB_stock_index=DH.DB_stockindex_build()     #股票指数日线数据
DB_MA=DH.DB_MA_build()          #均线数据
DB_FP=DH.DB_FP_build()          #基本面profit数据
DB_SL=DH.DB_SL_build()          #沪深股票代码
DB_SIL=DH.DB_SIL_build()        #沪深指数代码
DB_FO=DH.DB_FO_build()          #基本面operation数据
DB_FG=DH.DB_FG_build()          #基本面growth数据
DB_FB=DH.DB_FB_build()          #季频偿债能力
DB_FCF=DH.DB_FCF_build()         #季频现金流量
DB_FD=DH.DB_FD_build()          #季频杜邦指数
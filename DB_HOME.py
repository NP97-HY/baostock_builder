from sqlalchemy import create_engine
import pymysql


class DB_HOME(object):
    def __init__(self):
        pymysql.install_as_MySQLdb()
        
    def DB_stock_build(self):
        return create_engine("mysql://root:1qaz!QAZ@localhost:3306/stock?charset=utf8", max_overflow=5)

    def DB_MACD_build(self):
        return create_engine("mysql://root:1qaz!QAZ@localhost:3306/MACD?charset=utf8", max_overflow=5)

DH=DB_HOME()
DB_stock=DH.DB_stock_build()
DB_MACD=DH.DB_MACD_build()
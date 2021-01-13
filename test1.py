from database_manager.read_sql import *
from strategy.data_plot import *

# print(stock_day('sh.600333').low)


# first = ld[:len(ld)-5]>ld[2:len(ld)-3]
#     second = ld[1:len(ld)-4]>ld[2:len(ld)-3]
#     fouth = ld[3:len(ld)-2]>ld[2:len(ld)-3]
#     fifth = ld[4:len(ld)-1]>ld[2:len(ld)-3]
a=plot_xtf('600460')
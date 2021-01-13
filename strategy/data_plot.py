import matplotlib as mpl
# mpl.use('TkAgg')
from bokeh.plotting import figure, output_file, show
from database_manager.read_sql import *
import numpy as np
import matplotlib.pyplot as plt

def plot_f(code):
    data = stock_day(code)
    hd = np.array(data['high'])
    hd = np.insert(hd,0,[100000,100000])
    hd = np.insert(hd,len(hd)-1,[100000,100000])
    first = hd[:len(hd)-5]<hd[2:len(hd)-3]
    second = hd[1:len(hd)-4]<hd[2:len(hd)-3]
    fouth = hd[3:len(hd)-2]<hd[2:len(hd)-3]
    fifth = hd[4:len(hd)-1]<hd[2:len(hd)-3]
    high_plot = np.where(((first==1)&(second==1)&(fouth==1)&(fifth==1)),1,0)
    ld = np.array(data['low'])
    ld = np.insert(ld,0,[0,0])
    ld = np.insert(ld,len(ld)-1,[0,0])
    first = ld[:len(ld)-5]>ld[2:len(ld)-3]
    second = ld[1:len(ld)-4]>ld[2:len(ld)-3]
    fouth = ld[3:len(ld)-2]>ld[2:len(ld)-3]
    fifth = ld[4:len(ld)-1]>ld[2:len(ld)-3]
    low_plot = np.where(((first==1)&(second==1)&(fouth==1)&(fifth==1)),2,0)
    plot_d = high_plot+low_plot
    return plot_d,data

def plot_xtf(code):
    plot_d,data=plot_f(code)
    p = 0
    index = 0
    for i in range(plot_d.size):
        if plot_d[i] == 0:
            continue
        if plot_d[i] == 2 and p == 2:
            if data['low'][i]<data['low'][index]:
                plot_d[index] = 0
                index = i
            else:
                plot_d[i] = 0
        elif plot_d[i] == 1 and p == 1:
            if data['high'][i]>data['low'][index]:
                plot_d[index] = 0
                index = i
            else:
                plot_d[i] = 0
        else:
            p = plot_d[i]
            index = i
    x = []
    y = []
    for i in np.where((plot_d==1)|(plot_d==2))[0]:
        print(i)
        if i in np.where(plot_d==1)[0]:
            print(1)
            y.append(data['high'][i])
            x.append(i)
        else:
            y.append(data['low'][i])
            x.append(i)
    y=np.append(y, np.array(data['close'][len(plot_d)-1]))
    x=np.append(x,np.array(len(plot_d)-1))
    output_file("log_lines.html")
    p = figure(tools="pan,box_zoom,reset,save",
                y_axis_type="log", title=data.code[0],
                x_axis_label='sections', y_axis_label='particles'
                )
    p.line(x, y, color='navy', legend='avg')
    show(p)
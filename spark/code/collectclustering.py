#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
"""
import sys
import time
import math
import os 
import shutil
from operator import add
from pyspark import SparkContext

def raw_process(line):
    """Process the raw data to get hour,id of bs,imsi,latt,logt"""
    # day = "-".join([08,20+(line.strip().split('\t')[0]*100-1345392000000)/1000/3600/24])
    hour = int((float(line.strip().split('\t')[0])/3600-16)%24)
    ttime = long(float(line.strip().split('\t')[0])*1000)-long(1340000000000)
    id_bs = line.strip().split('\t')[2]
    imsi = line.strip().split('\t')[3]
    latt = float(line.strip().split('\t')[26])
    logt = float(line.strip().split('\t')[25])
    if  latt >=30.15 and latt <= 30.42 and logt >=120.02 and logt <=120.48:
        return (int(hour),str(imsi)+'\t'+str(ttime)+'\t'+str(id_bs)+'\t'+str(logt)+'\t'+str(latt))
    else :
        return (-1,"")


def remap(line):
    """Reshap the line for easy using for combing"""
    items = line.split('\t')
    return (items[0],(items[1],items[2]+'\t'+items[3]+'\t'+items[4]))


def add(a, b): 
    """Combine function"""
    return a +','+ str(b)


def process_person(line):
    """Get the attributes of  each person"""
    day = line.strip().split('\t')[0]
    hour = line.strip().split('\t')[1]
    imsi = line.strip().split('\t')[2]
    bs = line.strip().split('\t')[3]
    return ((str(day)+','+str(hour)+','+str(imsi)),str(bs))

def f(x): return x

def add(a, b): return a +','+ str(b)

def map(line):
    items = line.split('\t')
    return (items[0],items[1]+'\t'+items[2]+'\t'+items[3])

def regroup(z):
    """Regroup the bs in the style that one person pass from A to B"""
    y=[]
    
    i = 0
    while i<len(z):
        c = z[i:(i+2)]
        if len(c)>1 and c[0]!=c[1]:
            y.append(c)
        else:pass
        i+=1
    return y


def count_distance(line):
    """Get the distance between bs in km"""
    imsi = line[0]
    item1 = line[1][0].replace(')','').replace('\\"','').split(',')[1].replace('\'','').split('\\t')
    item2 = line[1][1].replace(')','').replace('\\"','').split(',')[1].replace('\'','').split('\\t')
    bsA,logtA,lattA = item1[0],float(item1[1]),float(item1[2])
    bsB,logtB,lattB = item2[0],float(item2[1]),float(item2[2])
    #Count the real distance between bs using Latitude and longitude
    if (logtA-logtB) != 0 and (lattA-lattB) != 0 and bsA != bsB:
        d = 6371*(math.acos(math.cos(lattA/180*math.pi)*math.cos(lattB/180*math.pi)*math.cos((logtA-logtB)/180*math.pi)+math.sin(lattA/180*math.pi)*math.sin(lattB/180*math.pi)))#d = #AB=R*arccos[cosAw*cosBw*cos(Aj-Bj)+sinAw*sinBw]
    else: d = 0
    return (str(bsA)+'\t'+str(bsB)+'\t'+str(d),str(imsi))


def count_weight(line):
    """Count the weight between bs """
    ci_1 = line[0].split('\t')[0]
    ci_2 = line[0].split('\t')[1]
    d = float(line[0].split('\t')[2])
    num = line[1][1]
    if d != 0:
        weight = num/d/1000
    else:
        weight = ""
    return str(ci_1)+'\t'+str(ci_2)+'\t'+str(d)+'\t'+str(num)+'\t'+str(weight)


if __name__ == "__main__":
    sc = SparkContext('spark://namenode.omnilab.sjtu.edu.cn:7077',appName="Process map")
    lines = sc.textFile('hdfs://namenode.omnilab.sjtu.edu.cn//user/chenxm/HZLOGS/SET0820',1)
    result = lines.map(lambda x : raw_process(x)) \
            .filter(lambda x: x[0] !=- 1) \
            .map(lambda x:x[1]) \
            .map(lambda x: remap(x)) \
            .combineByKey(str, add, add) \
            .map(lambda x:(str(x[0]),sorted(x[1].replace('"','').replace('(','').split('),')))) \
            .flatMapValues(regroup) \
            .map(lambda x: count_distance(x)) \
            .combineByKey(lambda value: (value, 1),
                                         lambda x, value: (x[0] + value, x[1] + 1),
                                         lambda x, y: (x[0] + y[0],x[1] + y[1])) 
    # result1 = result.map(lambda x:str(x[0].split('\t')[0]) + '\t' + str(x[0].split('\t')[1]) + '\t' + str(x[0].split('\t')[2]) + '\t' + str(x[1][1])) \
    #         .saveAsTextFile("/user/sunying/HangZhou/reprocess/8_20")
    #To output the distance and people traffic between bs
    
    # result2 = result.filter(lambda x:x[1][1] >= 5 ) \
    result1=result.map(lambda x:count_weight(x)) \
            .saveAsTextFile("/user/sunying/HangZhou/reprocess/8_20_distance_num_weight")
    #To output the weight between bs
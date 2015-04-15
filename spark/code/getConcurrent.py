import sys
import time
import math
import os 
import shutil
from operator import add
from pyspark import SparkConf
from pyspark import SparkContext

__author__ = 'sunying'

reload(sys)   
sys.setdefaultencoding('utf-8')  
def raw_data(line):
        "Get the start and end connect time "
        line_list = line.strip().split('\t')
        ttime = float(line_list[0])
        dtime = float(line_list[1])
        bs_id = line_list[2]
        imsi = line_list[3]
        latt = float(line_list[26])
        logt = float(line_list[25])

        if latt >=30.3012 and latt <= 30.3202 and logt >=120.0882 and logt <=120.0985:
            return (1,str(bs_id) + '\t'+str(imsi) + '\t'+str(ttime) + '\t'+str(dtime) )
        else :
            return (-1,"")

def time_depart(line):
        "'Get the connect hour minute second"
        time_list = set()
        items = line.split('\t')
        bs_id = items[0]
        imsi = items[1]
        ttime = float(items[2])
        dtime = float(items[3])
        # time_spend = dtime - ttime
        t = ttime
        while t <= dtime:
                time_transfer = get_hour_second(t)
                time_list.add(time_transfer)
                t += 1
        time_new = list(time_list)
        return( (str(bs_id) ,str(imsi) ),time_new)#.replace('[','').replace(']','').replace(',','\t')

def get_hour_second(time):
        day,hour,minute =get_day(int(float(time)))
        second = int((((float(time)/3600.0-int(float(time)*1000)/3600000))*60-int(((float(time)/3600.0-int(float(time)*1000)/3600000))*60))*6)
        return str(day)+'\t'+str(hour) + ':'+str(minute) +':'+str(second)

def get_day(value):
    format = '%Y-%m-%d,%H,%M'
    value = time.localtime(value)
    date = time.strftime(format, value)
    dt,ht,mt = date.split(',')
    return dt,ht,mt

# def depart_time(line):
#         id_bs,imsi = line[0][0],line[0][1]
#         time_list = line[1]
#         for time in time_list:
#             return ((id_bs,imsi),time)#str(id) + '\t'+ str(time)
def f(x):return x
def add(a, b): return a +','+ str(b)

def get_id_time(line):
        id_bs = line[0][0]
        imsi = line[0][1]
        time_list = line[1]
        for t in time_list:
            return ((id_bs,t),imsi)

def output_map(line):
        id_bs = line[0][0]
        time_str = line[0][1]
        day = time_str.split('\t')[0]
        hms = time_str.split('\t')[1].split(':')
        hour,minute,second = hms[0],hms[1],hms[2]
        count = line[1]
        return ((id_bs,day,hour),int(count))

if __name__ == "__main__":
    conf = (SparkConf()
                .setMaster("spark://namenode.omnilab.sjtu.edu.cn:7077")
                .setAppName("CountTimeValue")
                .set("spark.cores.max","128")
                .set("spark.driver.memory","8g")
                .set("spark.executor.memory","8g"))
    sc = SparkContext(conf = conf)
    lines = sc.textFile('hdfs://namenode.omnilab.sjtu.edu.cn//user/chenxm/HZLOGS/SET0821',1)
    result = lines.map(lambda x : raw_data(x)) \
            .filter(lambda x: x[0] !=- 1) \
            .map(lambda x:x[1])
            # .saveAsTextFile("/user/sunying/HangZhou/reprocess_again/concurrent/0821_result_hour_countmap345676")

   # result.saveAsTextFile("/user/sunying/HangZhou/reprocess_again/concurrent/0821_result")
    # result2 = result.map(lambda x :time_depart(x))\
    #                                         .map(lambda x: depart_time(x)) \
    #                                         .combineByKey(str,add,add) \
    #                                         .map(lambda (label,value):(label,list(set(value.replace('"','').strip().split(',')))))\
    #                                         .map(lambda x:get_id_time(x)) \
    #                                         .countByKey() #return hashmap
    # new = sorted(result2.iteritems(),key=lambda asd : asd[1],reverse = True)[0:10]

    result3 = result.map(lambda x :time_depart(x))\
                                            .flatMapValues(f) \
                                            .combineByKey(str,add,add) \
                                            .map(lambda (label,value):(label,list(set(value.replace('"','').strip().split(',')))))\
                                            .flatMapValues(f) \
                                            .map(lambda x: ((x[0][0],x[1]),1))\
                                            .reduceByKey(lambda a, b: a + b)\
                                            .map(lambda x: output_map(x)) \
                                            .combineByKey(int,
                                                                            lambda x, value: x+value,
                                                                            lambda x, y: x+y)\
                                            .map(lambda x:str(x[0][0])+'\t'+str(x[0][1])+'\t'+str(x[0][2])+'\t'+str(x[1]))
                                             # .combineByKey(lambda value: (value, 1),
                                             #                                lambda x, value: (x[0] + value, x[1] + 1),
                                             #                                lambda x, y: (x[0] + y[0], x[1] + y[1]))\ ## This is ok too
    result3.saveAsTextFile("/user/sunying/HangZhou/reprocess_again/concurrent/0821New2")
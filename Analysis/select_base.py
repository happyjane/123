import os
# from utils import DATA_DIR, CHART_DIR
import scipy as sp
import matplotlib.pyplot as plt
import pylab as pl


DATA_DIR = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), "data")
data = sp.genfromtxt(os.path.join(DATA_DIR,"base_count.csv"),delimiter = "\t")
#print data.shape
bs_dict = {}
for line in data:
	bs,connect = line[0],line[7]
	bs_dict.setdefault(bs,connect)
# print bs_dict
new_sort = sorted(bs_dict.iteritems(),key=lambda asd : asd[1],reverse = True)
print dict(new_sort[0:9])
# for i in new_sort:
# 	print bs_dict[i]
# print data.shape
import os
import scipy as sp
import matplotlib.pyplot as plt
import pylab as pl


DATA_DIR = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), "data")
data_bs = sp.genfromtxt(os.path.join(DATA_DIR,"base_count.csv"),delimiter = "\t")

def get_bs_sorted(data_bs):

	bs_dict = {}
	for line in data_bs:
		bs,connect = line[0],line[7]
		bs_dict.setdefault(bs,connect)
	# print bs_dict
	new_sort = sorted(bs_dict.iteritems(),key=lambda asd : asd[1],reverse = True)
	return new_sort
print get_bs_sorted(data_bs)[0]
data_failure = sp.genfromtxt(os.path.join(DATA_DIR,"failure_pro_0820.csv"),delimiter = "\t")
def get_failure_bs(data_failure):

	failure_dict = {}
	for line in data_failure:
		bs ,hour,sucess,response = line[0],line[1],line[2],line[3]
		retransfer,noreason,server,tcp = line[4],line[5],line[6],line[7]
		num = line[8]
		failure_dict.setdefault(bs,{})
		failure_dict[bs].setdefault(hour,{})
		failure_dict[bs][hour]["sucess"] = sucess
		failure_dict[bs][hour]["response"] = response
		failure_dict[bs][hour]["retransfer"] = retransfer
		failure_dict[bs][hour]["noreason"] = noreason
		failure_dict[bs][hour]["server"] = server
		failure_dict[bs][hour]["tcp"] = tcp
		failure_dict[bs][hour]["num"] = num
	return failure_dict
# select_bs = get_bs_sorted(data_bs)[0][0]
select_bs_infor = get_failure_bs(data_failure)[get_bs_sorted(data_bs)[11][0]]
# print select_bs_infor

x = []
y = []
z = []
for hour in select_bs_infor:
	z.append((select_bs_infor[hour]["num"],select_bs_infor[hour]["sucess"]))
	# x.append(select_bs_infor[hour]["num"])
	# y.append(select_bs_infor[hour]["sucess"])
z_new = sorted(z)
print z_new
x_new = []
y_new = []
i = 0
while i < len(z_new):
	x_new.append(z_new[i][0])
	y_new.append(z_new[i][1])
	i+= 1
pl.plot(x_new,y_new)
plt.show()



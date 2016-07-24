"""
This script tests the local VAV box control: relationship between HT%, CL%, cfm and rhv

@authour Shuyang Li <shuyangli@berkeley.edu>
"""
from smap.archiver.client import SmapClient
from smap.contrib import dtutil

import datetime, time, pdb, re
import numpy as np
import quantities as pq

from matplotlib import pyplot
from matplotlib import dates

##t = time.time()
t = time.strptime("30 Nov 15 23 40 00", "%d %b %y %H %M %S")
t = time.mktime(t)
c = SmapClient("http://www.openbms.org/backend")
source = "Metadata/SourceName = 'Sutardja Dai Hall BACnet'"
where = source + " and Path ~ 'S[0-9]-[0-9][0-9]' and" +\
        "(Path ~ 'CLG_LOOPOUT' or Path ~ 'HTG_LOOPOUT' or " +\
        "Path ~ 'AIR_VOLUME' or Path ~ 'VLV_POS' or " +\
        "Path ~ 'AI_3' or Path ~ '')"
tags = c.tags(where)
data = c.prev(where, t, streamlimit=1000, limit=1000)
vavs_rhv = [] # vav with reheat valve
vavs_rhv = ['S1-01', 'S1-02', 'S1-03', 'S1-04', 'S1-07', 'S1-08', 'S1-09', 'S1-10', 'S1-13', 'S1-15', 'S1-16', 'S1-17', 'S1-18', 'S1-19', 'S1-20', 'S2-01', 'S2-02', 'S2-03', 'S2-04', 'S2-05', 'S2-06', 'S2-07', 'S2-10', 'S2-11', 'S2-12', 'S2-13', 'S2-14', 'S2-15', 'S2-16', 'S2-17', 'S2-18', 'S2-19', 'S2-20', 'S2-21', 'S3-01', 'S3-02', 'S3-03', 'S3-04', 'S3-05', 'S3-06', 'S3-07', 'S3-08', 'S3-09', 'S3-10', 'S3-11', 'S3-12', 'S3-15', 'S3-16', 'S3-17', 'S3-18', 'S3-19', 'S3-20', 'S3-21', 'S4-01', 'S4-02', 'S4-03', 'S4-04', 'S4-05', 'S4-06', 'S4-07', 'S4-08', 'S4-09', 'S4-11', 'S4-12', 'S4-13', 'S4-15', 'S4-16', 'S4-18', 'S4-19', 'S4-20', 'S4-21', 'S5-01', 'S5-02', 'S5-03', 'S5-04', 'S5-05', 'S5-06', 'S5-07', 'S5-08', 'S5-09', 'S5-10', 'S5-11', 'S5-12', 'S5-13', 'S5-14', 'S5-16', 'S5-18', 'S5-19', 'S5-20', 'S5-21', 'S6-01', 'S6-02', 'S6-03', 'S6-04', 'S6-05', 'S6-06', 'S6-07', 'S6-08', 'S6-10', 'S6-11', 'S6-12', 'S6-13', 'S6-15', 'S6-17', 'S6-18', 'S6-19', 'S6-20', 'S7-01', 'S7-02', 'S7-03', 'S7-04', 'S7-05', 'S7-06', 'S7-07', 'S7-08', 'S7-09', 'S7-10', 'S7-13', 'S7-14', 'S7-15', 'S7-16']


for v in sorted(vavs_rhv):
    u_rhv = [tag['uuid'] for tag in tags if v in tag['Path'] and 'VLV_POS' in tag ['Path']]
    u_ht = [tag['uuid'] for tag in tags if v in tag['Path'] and 'HTG_LOOPOUT' in tag ['Path']]    
    u_dat = [tag['uuid'] for tag in tags if v in tag['Path'] and 'AI_3' in tag ['Path']]
    u_cl = [tag['uuid'] for tag in tags if v in tag['Path'] and 'CLG_LOOPOUT' in tag ['Path']]    
    u_cfm = [tag['uuid'] for tag in tags if v in tag['Path'] and 'AIR_VOLUME' in tag ['Path']]
    rhv = [np.array(d['Readings'])[:,1] for d in data if u_rhv[0] == d['uuid']][0]
    ht = [np.array(d['Readings'])[:,1] for d in data if u_ht[0] == d['uuid']][0]
    dat = [np.array(d['Readings'])[:,1] for d in data if u_dat[0] == d['uuid']][0]
    cl = [np.array(d['Readings'])[:,1] for d in data if u_cl[0] == d['uuid']][0]
    cfm = [np.array(d['Readings'])[:,1] for d in data if u_cfm[0] == d['uuid']][0]
    # relationship between rhv and HT%
##    if (any(rhv)|any(ht)):
##        fig = pyplot.figure()
##        ax1 = fig.add_subplot(111)
##        ax1.plot(ht,rhv,'ro',label = "rhv")
##        ax1.legend(loc=2)
##        ax1.set_ylabel('Reheat valve postiion')
####        ax2 = ax1.twinx()
####        ax2.plot(ht,dat,'bo',label = "dat")
####        ax2.legend(loc=1)
####        ax2.set_ylabel('Discharge air temperature')
##        ax1.set_xlabel('Heating loopout')
##        pyplot.title(["vav %s" %str(v)])
##        pyplot.show()
    # relationship between cfm and CL%
    if any(cfm)&(~all(ht)):
        fig = pyplot.figure()
##        pyplot.plot(cl,cfm,'ro')
        pyplot.scatter(cl,cfm,alpha = 0.1, s = 100,edgecolor='')
        pyplot.xlabel('Cooling loopout')
        pyplot.ylabel('Air flow rate')
        pyplot.title(["vav %s" %str(v)])
        pyplot.show()
        


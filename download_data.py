"""
This script analyzes the datastream of SDH
and writes the interested data into .csv file.

@author Shuyang Li <shuyangli@berkeley.edu>
"""

from smap.archiver.client import SmapClient
from datetime import datetime, date

import time
import numpy as np
import pandas as pd
import csv
import re
import pprint as pp
from matplotlib import pyplot, dates

c = SmapClient(base='http://new.openbms.org/backend',
               key=['XuETaff882hB6li0dP3XWdiGYJ9SSsFGj0N8'])

source = "Metadata/SourceName = 'Sutardja Dai Hall BACnet'"
restrict = source + " and Path ~ 'S[0-9]-[0-9][0-9]' and (Path ~ 'AIR_VOLUME' or  Path ~ 'AI_3' or Path ~ 'ROOM_TEMP')"
# start = date(2015, 10, 23)
# start_timestamp = time.mktime(start.timetuple()) 

t = time.strptime("10 Mar 16 13 40 00", "%d %b %y %H %M %S")
timestamp = time.mktime(t)

tags = c.tags(restrict)
data = c.prev(restrict, ref=timestamp, limit=1440, streamlimit=10000)
pp.pprint(data[0])
print 'length of data: '+str(len(data))


# name = 'zone_data.csv'
# f = open(name,'w')
# csv_writer = csv.writer(f)
# headers = ['Zone Name'
#            'Time', 
#            'Zone airflow [cfm]', 
#            'Zone discharge air temperature [F]',
#            'Zone temperature [F]']
# csv_writer.writerow(headers)
# f.close()

# To analyze zones with reheat
vavs_rhv = ['S1-01', 'S1-02', 'S1-03', 'S1-04', 'S1-07', 'S1-08', 'S1-09', 'S1-10', 'S1-13', 'S1-15', 'S1-16', 'S1-17', 'S1-18', 'S1-19', 'S1-20', 'S2-01', 'S2-02', 'S2-03', 'S2-04', 'S2-05', 'S2-06', 'S2-07', 'S2-10', 'S2-11', 'S2-12', 'S2-13', 'S2-14', 'S2-15', 'S2-16', 'S2-17', 'S2-18', 'S2-19', 'S2-20', 'S2-21', 'S3-01', 'S3-02', 'S3-03', 'S3-04', 'S3-05', 'S3-06', 'S3-07', 'S3-08', 'S3-09', 'S3-10', 'S3-11', 'S3-12', 'S3-15', 'S3-16', 'S3-17', 'S3-18', 'S3-19', 'S3-20', 'S3-21', 'S4-01', 'S4-02', 'S4-03', 'S4-04', 'S4-05', 'S4-06', 'S4-07', 'S4-08', 'S4-09', 'S4-11', 'S4-12', 'S4-13', 'S4-15', 'S4-16', 'S4-18', 'S4-19', 'S4-20', 'S4-21', 'S5-01', 'S5-02', 'S5-03', 'S5-04', 'S5-05', 'S5-06', 'S5-07', 'S5-08', 'S5-09', 'S5-10', 'S5-11', 'S5-12', 'S5-13', 'S5-14', 'S5-16', 'S5-18', 'S5-19', 'S5-20', 'S5-21', 'S6-01', 'S6-02', 'S6-03', 'S6-04', 'S6-05', 'S6-06', 'S6-07', 'S6-08', 'S6-10', 'S6-11', 'S6-12', 'S6-13', 'S6-15', 'S6-17', 'S6-18', 'S6-19', 'S6-20', 'S7-01', 'S7-02', 'S7-03', 'S7-04', 'S7-05', 'S7-06', 'S7-07', 'S7-08', 'S7-09', 'S7-10', 'S7-13', 'S7-14', 'S7-15', 'S7-16']
# for v in vavs_rhv:
for v in ['S1-01']:
    u_airflow = [tag['uuid'] for tag in tags if v in tag['Path'] and 'AIR_VOLUME' in tag['Path']]
    u_temp = [tag['uuid'] for tag in tags if v in tag['Path'] and 'ROOM_TEMP' in tag['Path']]
    u_dat = [tag['uuid'] for tag in tags if v in tag['Path'] and 'AI_3' in tag['Path']]
    airflow = [np.array(d['Readings'])[:,1] for d in data if u_airflow[0] == d['uuid']][0]
    airflow_test = [np.array(d['Readings']) for d in data if u_airflow[0] == d['uuid']][0]
    print airflow_test[-1,1]
    temp = [np.array(d['Readings'])[:,1] for d in data if u_temp[0] == d['uuid']][0]
    dat = [np.array(d['Readings'])[:,1] for d in data if u_dat[0] == d['uuid']][0]
    tm = [np.array(d['Readings'])[:,0] for d in data if u_temp[0] == d['uuid']][0]
    tm_dt = [datetime.fromtimestamp(t/1000).strftime('%Y-%m-%d %H:%M:%S') for t in tm]
    d = {'ZoneTemperature[F]':pd.Series(temp,index = tm_dt),
         'DischargeAirTemperature[F]':pd.Series(dat,index = tm_dt),
         'AirFlow[cfm]':pd.Series(airflow,index = tm_dt)}
    df = pd.DataFrame(d)
    df.to_csv('D:/GitHub/turnkey_bacnet_apps/tmp/data/'+v+'.csv')




data_test = {}
data_test['-'.join(['AH2A','SAT','uuid'])] = u_dat[-1]
data_test['-'.join(['AH2B','SAT','uuid'])] = u_dat[-1]
download_data = [np.array(d['Readings']) for d in data if d['uuid'] == u_dat[-1]][0]
print data_test
print download_data
print download_data[0]


test_list = np.array([1,2,3,4,5,1])
print np.where(test_list == 3)[0]
print np.where(test_list == 3)[0][0]
print np.unique(test_list)


diff_sat_input = [-0.2,0,0.2]
diff_sat = np.asarray(map(float,[-0.2,0,0.2]))
print diff_sat
print diff_sat_input


estimations = np.zeros(3)
estimations += np.array([1,2,3])
print estimations
# # plot
# for d1 in data:
#   pyplot.plot_date(dates.epoch2num(tm/1000), np.array(d1['Readings'])[:,1], '-',
#                    tz='America/Los_Angeles')
# pyplot.show()

# for v in vavs_rhv:
#   print '\nVAV box: '+str(v)
#   u_airflow = [tag['uuid'] for tag in tags if v in tag['Path'] and 'AIR_VOLUME' in tag['Path']]
#   u_temp = [tag['uuid'] for tag in tags if v in tag['Path'] and 'ROOM_TEMP' in tag['Path']]
#   u_dat = [tag['uuid'] for tag in tags if v in tag['Path'] and 'AI_3' in tag['Path']]
#   airflow = [np.array(d['Readings'])[:,1] for d in data if u_airflow[0] == d['uuid']][0]
#   dat = [np.array(d['Readings'])[:,1] for d in data if u_dat[0] == d['uuid']][0]
#   temp = [np.array(d['Readings'])[:,1] for d in data if u_temp[0] == d['uuid']][0]
#   newrow = [v,tm,airflow,dat,temp]
#   csv_writer.writerow(newrow)

# f.close()



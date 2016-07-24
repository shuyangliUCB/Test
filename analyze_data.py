"""
This script analyzes the datastream of SDH
and testing results of the new cost based SAT reset strategy.

@author Shuyang Li <shuyangli@berkeley.edu>
"""
from smap.archiver.client import SmapClient

from datetime import datetime
import time
import numpy as np
import pandas as pd
import csv
import re
import pprint as pp


from matplotlib import pyplot, dates
# from bokeh.plotting import figure, output_file, show
import plotly
import plotly.plotly as py
import plotly.graph_objs as go
plotly.tools.set_credentials_file(username='shuyang', api_key='cfxnnzqsea')
from plotly.graph_objs import Bar, Scatter, Figure, Layout

class AnalyzeData(object):
    def __init__(self):
        # vav,chw,vfd naming
        self.vavs_rhv = [
            'S1-01', 'S1-02', 'S1-03', 'S1-04', 'S1-07', 'S1-08', 'S1-09', 'S1-10', 'S1-13', 'S1-15', 'S1-16', 'S1-17', 'S1-18', 'S1-19', 'S1-20',
            'S2-01', 'S2-02', 'S2-03', 'S2-04', 'S2-05', 'S2-06', 'S2-07', 'S2-10', 'S2-11', 'S2-12', 'S2-13', 'S2-14', 'S2-15', 'S2-16', 'S2-17', 'S2-18', 'S2-19', 'S2-20', 'S2-21',
            'S3-01', 'S3-02', 'S3-03', 'S3-04', 'S3-05', 'S3-06', 'S3-07', 'S3-08', 'S3-09', 'S3-10', 'S3-11', 'S3-12', 'S3-15', 'S3-16', 'S3-17', 'S3-18', 'S3-19', 'S3-20', 'S3-21',
            'S4-01', 'S4-02', 'S4-03', 'S4-04', 'S4-05', 'S4-06', 'S4-07', 'S4-08', 'S4-09', 'S4-11', 'S4-12', 'S4-13', 'S4-15', 'S4-16', 'S4-18', 'S4-19', 'S4-20', 'S4-21',
            'S5-01', 'S5-02', 'S5-03', 'S5-04', 'S5-05', 'S5-06', 'S5-07', 'S5-08', 'S5-09', 'S5-10', 'S5-11', 'S5-12', 'S5-13', 'S5-14', 'S5-16', 'S5-18', 'S5-19', 'S5-20', 'S5-21',
            'S6-01', 'S6-02', 'S6-03', 'S6-04', 'S6-05', 'S6-06', 'S6-07', 'S6-08', 'S6-10', 'S6-11', 'S6-12', 'S6-13', 'S6-15', 'S6-17', 'S6-18', 'S6-19', 'S6-20',
            'S7-01', 'S7-02', 'S7-03', 'S7-04', 'S7-05', 'S7-06', 'S7-07', 'S7-08', 'S7-09', 'S7-10', 'S7-13', 'S7-14', 'S7-15', 'S7-16'
        ]
        self.vavs_no_rhv = [
            'S1-05', 'S1-06', 'S1-14', 'S2-08', 'S2-09', 'S3-13', 'S3-14', 'S4-10', 'S4-14', 'S4-17',
            'S5-15', 'S5-17', 'S6-09', 'S6-14', 'S6-16', 'S7-11', 'S7-12'
        ]
        self.vavs = self.vavs_rhv + self.vavs_no_rhv
        self.chw_coils = ['AH2A', 'AH2B']
        self.vfds = ['AH2A', 'AH2B']
        # define component fields
        self.components_by_type = {
            'vfds': self.vfds,
            'chw_coils': self.chw_coils,
            'vavs_rhv': self.vavs_rhv,
            'vavs_no_rhv': self.vavs_no_rhv
        }
        self.datapoints_by_type = {
            'vfds': ['SF_VFD:POWER'],
            'chw_coils': ['SAT', 'MAT', 'CCV', 'SF_CFM'],
            'vavs_no_rhv': ['AIR_VOLUME', 'CTL_FLOW_MIN', 'CTL_FLOW_MAX', 'CTL_STPT'],
            'vavs_rhv': ['AIR_VOLUME', 'CTL_FLOW_MIN', 'CTL_FLOW_MAX', 'CTL_STPT', 'VLV_POS',
                         'CLG_LOOPOUT', 'HTG_LOOPOUT', 'AI_3', 'rhv_closed_temp_change']
        }
        self.unit_of_estimated_data = {
            'rhw_cost': '$/hr', 'fan_cost': '$/hr', 'chw_cost': '$/hr', 'tot_cost': '$/hr',
            'chw_power': 'kW', 'chw_power_AH2A': 'kW', 'chw_power_AH2B': 'kW', 'rhw_power': 'kW',
            'fan_power': 'kW', 'fan_power_from_regression_curve': 'kW',
            'fan_power_AH2A': 'kW', 'fan_power_AH2B': 'kW', 'ahu_afr': 'cfm'
        }
        # define data restriction
        self.restrict_central = "Metadata/SourceName = 'Sutardja Dai Hall BACnet' and Path ~ 'AH2' and " \
            + "(Path ~ 'SAT' or Path ~ 'MAT' or Path ~ 'SF_CFM' or Path ~ 'CCV' or Path ~ 'SF_VFD:POWER') and " \
            + "not (Path ~ 'STP' or Path ~ 'RAH')"
        self.restrict_local = "Metadata/SourceName = 'Sutardja Dai Hall BACnet' and Path ~ 'S[0-9]-[0-9][0-9]' and " \
           + "not (Path ~ 'act') and (Path ~ 'AI_3' or Path ~ 'VLV_POS' or Path ~ 'HTG_LOOPOUT' or " \
           + "Path ~ 'CLG_LOOPOUT' or Path ~ 'AIR_VOLUME' or Path ~ 'ROOM_TEMP' or " \
           + "Path ~ 'CTL_FLOW_MIN' or Path ~ 'CTL_FLOW_MAX' or Path ~ 'CTL_STPT')"
        self.restrict_result = "Metadata/SourceName = 'Sutardja Dai Hall SAT Reset' and Path ~ 'SAT_Reset_debug_20160719' and " \
           + "Path ~ 'rhv_closed_temp_change' or Path ~ 'computed_sat' and " \
           + "not Path ~ 'SAT_Reset_debug_20160719_C'"
        self.restrict_new = "(%s) or (%s) or (%s)" % \
                            (self.restrict_central, self.restrict_local, self.restrict_result)
        self.restrict_oat = "Metadata/SourceName = 'Sutardja Dai Hall BACnet' and Path = '/Siemens/SDH.PXCM-08/SDH/OAT'"
        # creat smap client object
        self.archiver_url = 'http://new.openbms.org/backend'
        self.smap_client = SmapClient(self.archiver_url)
        # parameters
        self.limit = 40

    def download_data(self,timestamp):
        # store uuid and data
        self.data = {}
        tags = self.smap_client.tags(self.restrict_new)
        fetched_data = self.smap_client.prev(self.restrict_new, ref=timestamp, streamlimit=10000, limit=self.limit)
        print 'Total number of data stream is: %s' % len(fetched_data)
        for component_type in self.datapoints_by_type.keys():
            for component in self.components_by_type[component_type]:
                for datapoint in self.datapoints_by_type[component_type]:
                    uuid = [tag['uuid'] for tag in tags if (datapoint in tag['Path'] and component in tag['Path'])][0]
                    download_data = [np.array(d['Readings']) for d in fetched_data if d['uuid'] == uuid][0]
                    self.data['-'.join([component, datapoint, 'uuid'])] = uuid
                    self.data['-'.join([component, datapoint, 'value'])] = download_data[:,1]
                    self.data['-'.join([component, datapoint, 'time'])] = download_data[:,0]
                    self.data['-'.join([component, datapoint, 'dt'])] = [datetime.fromtimestamp(t/1000).strftime('%Y-%m-%d %H:%M:%S') for t in download_data[:,0]]
                    # print 'Time of %s: %s' % \
                    #       ('-'.join([component, datapoint]), self.data['-'.join([component, datapoint, 'dt'])][0])
        oat = self.smap_client.prev(self.restrict_oat, ref=timestamp)[0]['Readings'][0]
        self.data['OAT'] = oat[:,1]
        self.data['OAT-dt'] = [datetime.fromtimestamp(t/1000).strftime('%Y-%m-%d %H:%M:%S') for t in oat[:,0]]
        u_sat = [tag['uuid'] for tag in tags if ('computed_sat' in tag['Path'])][0]
        computed_sat = [np.array(d['Readings']) for d in fetched_data if d['uuid'] == u_sat][0]
        self.data['computed_sat'] = computed_sat[:,1]
        self.data['computed_sat-time'] = computed_sat[:,0]
        self.data['computed_sat-dt'] = [datetime.fromtimestamp(t/1000).strftime('%Y-%m-%d %H:%M:%S') for t in computed_sat[:,0]]
        self.data['SAT-avg-value'] = np.mean([self.data[coil + '-SAT-value'] for coil in self.chw_coils], axis=0)
        self.data['MAT-avg-value'] = np.mean([self.data[coil + '-MAT-value'] for coil in self.chw_coils], axis=0)
        self.data['AFR-ahu-tot-value'] = np.sum([self.data[coil + '-SF_CFM-value'] for coil in self.chw_coils], axis=0)
        self.data['AFR-zone-tot-value'] = np.sum([self.data[vav + '-AIR_VOLUME-value'] for vav in self.vavs], axis=0)
        return self.data
# tm = [np.array(d['Readings'])[:,0] for d in fetched_data if uuid == d['uuid']][0]
# tm_dt = [datetime.fromtimestamp(t/1000).strftime('%Y-%m-%d %H:%M:%S') for t in tm]
# data['time'] = tm_dt


## power estimation
def calc_heat_flow(volumetric_flow,delta_temperature):
    rho = 1.2005 # density of air @ 20degC [Unit: kg/m^3]
    c = 1005 # specific heat capacity of air @ 20degC [Unit: J/kg/K]
    return volumetric_flow * delta_temperature * c * rho / 1.8 / 2118.88 / 1000  # [Unit: kW] 


# estimations   = {}
# # calculate reheat power
# estimations['Total_rhw_power'] = np.zeros(limit)
# for vav in vavs_rhv: # VAV boxes with reheat
#   afr = data[vav + '-AIR_VOLUME-value'][:,1]
#   ctl_stpt = data[vav + '-CTL_STPT-value'][-1,1]
#   dat  = data[vav + '-AI_3-value'][:,1]
#   rhv  = data[vav + '-VLV_POS-value'][:,1]
#   clg  = data[vav + '-CLG_LOOPOUT-value'][:,1]
#   htg  = data[vav + '-HTG_LOOPOUT-value'][:,1]
#   diff_temp = dat-data[vav + '-rhv_closed_temp_change-value'][:,1]-data['SAT-avg-value'] 
#   zone_rhw_power = np.maximum(0, calc_heat_flow(afr, diff_temp))
#   zone_rhw_power[np.where(htg==0)] = 0
#   estimations['Total_rhw_power']  += zone_rhw_power
#   if zone_rhw_power.any() > 0:
#     print str(vav)+'_rhw_power: %s' % str(zone_rhw_power)
#   estimations['Total_rhw_cost'] = estimations['Total_rhw_power']*0.023
# print 'Total_rhw_power: %s' % estimations['Total_rhw_power']
# print 'Total_rhw_cost: %s' % estimations['Total_rhw_cost']
# print 'Time: %s' % data['time']
# # # plot
# # pyplot.plot_date(dates.epoch2num(tm/1000), estimations['Total_rhw_power'], '-',
# #                    tz='America/Los_Angeles')
# # pyplot.show()


if __name__=='__main__':
    t = time.strptime("20 Jul 16 9 20 00", "%d %b %y %H %M %S")
    timestamp = time.mktime(t)
    obj = AnalyzeData()
    data = obj.download_data(timestamp)
    # plot
    computed_sat = go.Scatter(
        x = data['computed_sat-dt'],
        y = data['computed_sat'],
        mode = 'lines+markers',
        name = 'computed_sat'
    )
    measured_sat = go.Scatter(
        x = data['AH2A-SAT-dt'],
        y = data['SAT-avg-value'],
        mode = 'lines+markers',
        name = 'measured_sat-avg'
    )
    data = [computed_sat, measured_sat]
    py.iplot(data, filename='scatter-mode')
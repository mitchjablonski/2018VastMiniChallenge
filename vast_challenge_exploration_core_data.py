# -*- coding: utf-8 -*-
"""
Created on Mon Jul 23 19:28:48 2018

@author: mitch
"""

from __future__ import division
from __future__ import print_function
from datetime import datetime

import pandas as pd
from collections import defaultdict
import matplotlib.pyplot as plt
import seaborn as sns
import date_time_conversion as dt_converter




'''
0 is for calls
1 is for emails
2 is for purchases
3 is for meetings
'''
'''
calls.csv has information on 10.6 million calls (251 MB uncompressed)
emails.csv has information on 14.6 million emails (345 MB uncompressed)
purchases.csv has information on 762 thousand purchases (18.8 MB uncompressed)
meetings.csv has information on 127 thousand meetings (3.26 MB uncompressed)
'''

#For correlating the data together, we should see the frequnecy of the data for those that appear in the suspicious set
#After we have a view of the data, we should look at the larger data set filtered on only data that includes them

def investigate_data(input_df, data_type):
    temp_data = pd.DataFrame()
    temp_data['data_frequency'.format(data_type)] = input_df.month_yr.value_counts()
    temp_data['month_yr'] = temp_data.index
    temp_data['data_type'] = data_type
    return temp_data


#Data is over a time period of 2.5yrs
if __name__ == '__main__':
    columns = ['Source', 'Etype', 'Destination', 'TimeStamp']
    data_types = ['calls', 'emails', 'purchases', 'meetings']
    data_splits = {}
    start_time = 1431316800
    use_preprocess = True
    #data_types = ['meetings']
    described_data = pd.DataFrame(columns=['data_frequency','month_yr','data_type'])
    for data in data_types:
        print(data)
        if use_preprocess:
            curr_data = pd.read_csv('date_timed_{}.csv'.format(data))
        else:
            curr_data = pd.read_csv(data+'.csv', header=None, names = columns)
            dt_converter.convert_time(curr_data)
            curr_data.to_csv('date_timed_{}.csv'.format(data))
        described_data = described_data.append(investigate_data(curr_data,data), ignore_index=True)
    described_data.to_csv('data_described_frequency_v2.csv')
    
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 23 19:28:48 2018

@author: mitch
"""

from __future__ import division
from __future__ import print_function

import pandas as pd
from collections import defaultdict
import matplotlib.pyplot as plt
import seaborn as sns


def get_frequency_counts(df):
    ##Probabaly better to find the overall min and max and use that across all data
    
    min_time = min(df['TimeStamp'])
    max_time = max(df['TimeStamp'])
    print(max_time)
    #Should probably use min_time = 0
    #Max time = 2.5 * 366 * 24 * 60 *60
    time_split = 2.5*4 #number of quarters data exists over
    time_delta = max_time - min_time
    time_steps = time_delta/time_split
    data_splits = {}
    time_splits = []
    frequency_counts = {}#defaultdict(list)
    unique_from = {}
    unique_to = {}
    for i in range(int(time_split)):
        start_time = min_time + (i) * time_steps
        end_time = min_time + (i + 1) * time_steps
        parse_data(df, i, start_time, end_time, 
                   frequency_counts, unique_from,
                   unique_to, data_splits)
        time_splits.append([start_time, end_time])
    return data_splits, time_splits

##Need a good way of determining the days of the week that the data occurs on, and the hour
##Data starts May 11th 2015, at 14:00
#Due to this, lets normalize the data and add 50400 seconds to all timestamps to make it easier to reference. time 0
##Normal workday is : 5am-8pm 05:00- 20:00
##So times 0-18000 seconds in the weekday are outside the workday, and 
#Times 72000-86400 are outsides the workday
##86400 seconds in a day, first day begins at 
#50400 seconds into the day
#604800 seconds in the week 
#The first 432000 seconds are the work week.
#So if we do say 650000 % 604800 we get just the remainder, making it easier to deal in just weeks
#same could be applied to days ie 

def parse_data(df, quarter, start_time, end_time, frequency_counts, 
               unique_from, unique_to, data_splits):
    curr_df = df[(df['TimeStamp'] > start_time) & (df['TimeStamp'] < end_time)]
    freq,_ = curr_df.shape
    #frequency_counts['Q{}'.format(1+i)] = freq
    uni_from = curr_df['Source'].nunique()
    uni_to = curr_df['Destination'].nunique()
    frequency_counts[quarter+1] = freq
    unique_from[quarter+1] = uni_from
    unique_to[quarter+1] = uni_to
    data_splits['frequency'] = frequency_counts
    data_splits['unique_source'] = unique_from
    data_splits['unique_destination'] = unique_to

def get_frequency_counts_sus(df, time_splits):
    data_splits = {}
    frequency_counts = {}#defaultdict(list)
    unique_from = {}
    unique_to = {}
    for quarter, times in enumerate(time_splits):
        start_time = times[0]
        end_time = times[1]
        parse_data(df, quarter, start_time, end_time, 
                   frequency_counts, unique_from, unique_to, data_splits)
    return data_splits

def describe_data(data_splits, data):
    print(data_splits)
    sns.set_style()
    fig = plt.gcf()
    for keys in data_splits.keys():
        fig = plt.gcf()
        lists = sorted(data_splits[keys].items())
        x, y = zip(*lists)
        plt.xlabel('Quarter of Business')
        plt.ylabel('{}'.format(keys))
        plt.title(data)
        plt.plot(x, y)
        plt.show()
        fig.savefig('Plots/{}{}.png'.format(data, keys), dpi = 900)
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



#Data is over a time period of 2.5yrs
if __name__ == '__main__':
    columns = ['Source', 'Etype', 'Destination', 'TimeStamp']
    data_types = ['calls', 'emails', 'purchases', 'meetings']
    data_splits = {}
    for data in data_types:
        data_splits, time_splits = get_frequency_counts(pd.read_csv(data+'.csv', header=None, names = columns))
        describe_data(data_splits,data)
        data_splits = get_frequency_counts_sus(pd.read_csv('Suspicious_{}.csv'.format(data), header=None, names = columns), time_splits)
        describe_data(data_splits, 'Suspicious_{}'.format(data))
    #describe_data(data_splits)
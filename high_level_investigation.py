# -*- coding: utf-8 -*-
"""
Created on Sun Aug 12 12:14:12 2018

@author: mitch
"""
import pandas as pd

def investigate_data(input_df, data_type):
    output_data = pd.DataFrame()
    #temp_data['data_amount'.format(data_type)] = input_df.month_yr.value_counts()
    #temp_data['month_yr'] = temp_data.index
    #temp_data['data_type'] = data_type
    grouped = input_df.groupby('month_yr')
    #unique = grouped['Source'].nunique()
    prev_month_unique_source = []
    prev_month_unique_dest   = []
    curr_month_unique_source = []
    curr_month_unique_dest   = []
    for name, group in grouped:
        temp_data = pd.DataFrame()
        temp_data['data_amount'] = pd.Series(group.agg('count')[0]) # count the # of elements that exist
        temp_data['month_yr']    = pd.Series(name)
        temp_data['data_type']   = pd.Series(data_type)
        temp_data['num_unique_source'] = pd.Series(group['Source'].nunique())
        temp_data['num_unique_dest']   = pd.Series(group['Destination'].nunique())
        temp_data['avg_time_between_interactions'] = pd.Series(group['TimeStamp'].diff().mean())
        curr_month_unique_source =  group['Source'].unique()
        curr_month_unique_dest   = group['Destination'].unique()
        temp_data['change_in_unique_source'] = pd.Series(len(
                                                set(curr_month_unique_source) - set(prev_month_unique_source)))
        temp_data['change_in_unique_dest'] = pd.Series(len(
                                                set(curr_month_unique_dest) - set(prev_month_unique_dest)))
        output_data = output_data.append(temp_data, ignore_index=True)
        prev_month_unique_source = curr_month_unique_source
        prev_month_unique_dest   = curr_month_unique_dest
    return output_data
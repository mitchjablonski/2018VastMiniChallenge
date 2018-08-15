# -*- coding: utf-8 -*-
"""
Created on Sun Aug 12 12:14:12 2018

@author: mitch
"""
import pandas as pd
import numpy as np

def investigate_data(input_df, data_type):
    output_data = pd.DataFrame()
    input_df = input_df.copy()
    input_df.sort_values(by='full_date', inplace=True)
    grouped = input_df.groupby('month_yr')
    prev_month_unique_source = []
    prev_month_unique_dest   = []
    prev_month_unique_ids    = []
    curr_month_unique_ids    = []
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
        
        curr_month_unique_ids = np.concatenate((curr_month_unique_source, curr_month_unique_dest))
        curr_month_unique_ids = np.unique(curr_month_unique_ids)
        
        temp_data['num_unique_ids']          = pd.Series(len(curr_month_unique_ids))
        temp_data['change_in_unique_ids']    = pd.Series(len(
                                                        set(curr_month_unique_ids) - set(prev_month_unique_ids)))
        temp_data['change_in_unique_source'] = pd.Series(len(
                                                set(curr_month_unique_source) - set(prev_month_unique_source)))
        temp_data['change_in_unique_dest'] = pd.Series(len(
                                                set(curr_month_unique_dest) - set(prev_month_unique_dest)))
        output_data = output_data.append(temp_data, ignore_index=True)
        prev_month_unique_source = curr_month_unique_source
        prev_month_unique_dest   = curr_month_unique_dest
        prev_month_unique_ids = curr_month_unique_ids
    return output_data
# -*- coding: utf-8 -*-
"""
Created on Mon Aug 13 21:41:48 2018

@author: mitch
"""
import math
import pandas as pd
import numpy as np

def purchase_analysis(purchase_row, input_df, layers, output_dict, suspicious_indicator):
    ##We will want to get all of their interactions that occured within a one month timeframe
    look_at_size_of_network_X_layers_out(input_df, purchase_row, layers, output_dict, suspicious_indicator)
    source = purchase_row['Source']
    destination = purchase_row['Destination']
    purchase_time = purchase_row['TimeStamp']
    seconds_in_month = int(2.628e+6)
    #seconds_in_five_days = int(432000)
    ##TODO should we look at a longer time period?  Our suspicious data goes
    ##Back one year, and forward 6 months
    time_window = seconds_in_month
    filtered_df = time_filter_df(input_df, time_window, time_window, purchase_time)
    
    ##Our destination in the suspicous purchase, is actaully the only interaction
    #they have in the suspicious dataset, we should potentially expect the purchasing agent(destination)
    #to have little contact with the soruce
    source_dest = (filtered_df['Source'] == source) | (filtered_df['Destination'] == destination)
    dest_source = (filtered_df['Source'] == destination) | (filtered_df['Destination'] == source)
    filtered_df = filtered_df[source_dest | dest_source]
    
    ##TODO should this go out another layer?  is there more info there?
    filtered_df = filtered_df.append(purchase_row)
    
    return filtered_df

def determine_metrics_for_purchase(input_df):
    input_df = input_df.copy()
    unique_sources = input_df['Source'].nunique()
    unique_destinations = input_df['Destination'].nunique()
    total_interactions  =input_df.count()[0]
    int_per_source = math.ceil(total_interactions/unique_sources)
    int_per_dest   = math.ceil(total_interactions/unique_destinations)
    input_df['full_date'] = pd.to_datetime(input_df['full_date'])
    input_df.sort_values(by='full_date', inplace=True)
    mean_time_between  = input_df['full_date'].diff().mean()
    return (unique_sources, unique_destinations, 
            total_interactions, int_per_source, 
            int_per_dest, mean_time_between)
    
def determine_layers_out(input_df, purchase_row, output_dict):
    output_df = input_df.copy()    
    source_dest = (output_df['Source'].isin([purchase_row['Source']])) | (output_df['Destination'].isin([purchase_row['Destination']]))
    dest_source = (output_df['Source'].isin([purchase_row['Destination']])) | (output_df['Destination'].isin([purchase_row['Source']]))
    temp_df = output_df[source_dest | dest_source]
    #temp_df = output_df[source_dest]
    print('determine_layers_out')
    layers = 0
    full_cols, _ = output_df.shape
    temp_cols, _ = temp_df.shape
    while temp_cols < full_cols:
        source_dest = (output_df['Source'].isin(temp_df['Source'])) | (output_df['Destination'].isin(temp_df['Destination']))
        dest_source = (output_df['Source'].isin(temp_df['Destination'])) | (output_df['Destination'].isin(temp_df['Source']))
        temp_df = output_df[source_dest | dest_source]
        #temp_df = output_df[source_dest]
        layers += 1
        temp_cols, _ = temp_df.shape
    
    describe_network_interactions(temp_df, purchase_row, output_dict)
    
    print('Forcing layers to be 1')
    layers=1
    print(layers)
    return layers

def describe_network_interactions(temp_df, purchase_row, output_dict, suspicious_indicator):
    row, columns = temp_df.shape
    new_df = temp_df.set_index('full_date')
    
    rolling_window_counts = new_df.rolling(window='8D', min_periods=1)['TimeStamp'].count()
    primary_source_int = temp_df[(temp_df['Destination'] == purchase_row['Source']) | 
                                    (temp_df['Source'] == purchase_row['Source'])]['Source'].count()
    #if ((row < 12000) and row > 20 and (rolling_window_counts.max() < 300) and
    #           primary_source_int < 80):  ##Our code has return at most 8000 for our dataset
    if True:
        temp_df['full_date'] = pd.to_datetime(temp_df['full_date'])
        temp_df.sort_values(by='full_date', inplace=True)
        time_between_interactions  = temp_df['full_date'].diff()
    
        print('Mean time between interactions {}'.format(time_between_interactions.mean()))
        print('Max time between interactions {}'.format(time_between_interactions.max()))
        print('Min time between interactions {}'.format(time_between_interactions.min()))
        rolling_diff  = temp_df['full_date'].diff(periods=5)
        print('Mean time rolling_diffs {}'.format(rolling_diff.mean()))
        print('Max time rolling_diffs {}'.format(rolling_diff.max()))
        print('Min time rolling_diffs {}'.format(rolling_diff.min()))
    

        print('Mean number of entries in window {}'.format(rolling_window_counts.mean()))
        print('Max number of entries in window {}'.format(rolling_window_counts.max()))
        print('Min number of entries in window {}'.format(rolling_window_counts.min()))
    
        unique_source_dest = np.concatenate((temp_df['Source'].unique(), temp_df['Destination'].unique()))
        unique_source_dest = np.unique(unique_source_dest)
        
        primary_dest_int = temp_df[(temp_df['Destination'] == purchase_row['Destination']) | 
                                    (temp_df['Source'] == purchase_row['Destination'])]['Destination'].count()
    
        mean_dest = temp_df['Destination'].value_counts().mean()
        mean_source = temp_df['Source'].value_counts().mean()
        unique_source = temp_df['Source'].nunique()
        unique_dest = temp_df['Destination'].nunique()
        tot_entries = temp_df['Source'].count()
        '''
        print('Primary source interactions {}'.format(primary_source_int))
        print('Primary dest interactions {}'.format(primary_dest_int))
        print('Average dest count {}'.format(mean_dest))
        print('Average source count {}'.format(mean_source))
        print('Number unique Sources {}'.format(unique_source))
        print('Number Unique Destination {}'.format(unique_dest))
        print('Unique combined source and dest {}'.format(unique_source_dest.size))
        print('Total Number of entries {}'.format(tot_entries))
        '''
        output_dict['Source'].append(purchase_row['Source'])
        output_dict['Destination'].append(purchase_row['Destination'])
        output_dict['Primary_source_interactions'].append(primary_source_int)
        output_dict['Primary_dest_interactions'].append(primary_dest_int)
        output_dict['Avg_dest_count'].append(mean_dest)
        output_dict['Avg_source_count'].append(mean_source)
        output_dict['num_uniq_source'].append(unique_source)
        output_dict['num_uniq_dest'].append(unique_dest)
        output_dict['combined_unique'].append(unique_source_dest.size)
        output_dict['total_entires'].append(tot_entries)
        output_dict['suspicious_indicator'].append(suspicious_indicator)

def look_at_size_of_network_X_layers_out(input_df, purchase_row, layers, output_dict, suspicious_indicator):
    temp_layers = 0 
    output_df = input_df.copy()
    ##TODO is time limiting good here?
    ##Our inital data set only includes a full year behind, and one year ahead
    seconds_in_month = int(2.628e+6)
    one_year_back = seconds_in_month*12
    six_months_fwrd = seconds_in_month*6
    seconds_in_five_days = int(432000)
    
    ##Do the inital pass with only a 5 day window, and do the second pass with
    #a 1 yr/six month window
    minimal_time_df = time_filter_df(output_df, seconds_in_five_days, seconds_in_five_days, purchase_row['TimeStamp'])
    ##TODO
    source_dest = (minimal_time_df['Source'].isin([purchase_row['Source']])) | (minimal_time_df['Destination'].isin([purchase_row['Destination']]))
    dest_source = (minimal_time_df['Source'].isin([purchase_row['Destination']])) | (minimal_time_df['Destination'].isin([purchase_row['Source']]))
    temp_df = minimal_time_df[source_dest | dest_source]
    #temp_df = output_df[source_dest]
    while temp_layers < layers:
        output_df = time_filter_df(output_df, six_months_fwrd, one_year_back, purchase_row['TimeStamp'])
        source_dest = (output_df['Source'].isin(temp_df['Source'])) | (output_df['Destination'].isin(temp_df['Destination']))
        dest_source = (output_df['Source'].isin(temp_df['Destination'])) | (output_df['Destination'].isin(temp_df['Source']))
        temp_df = output_df[source_dest | dest_source]
        #temp_df = output_df[source_dest]
        temp_layers += 1
        
    temp_dest = temp_df['Destination'].value_counts()
    temp_source = temp_df['Source'].value_counts()
    all_comms = temp_source.add(temp_dest, fill_value = 0)
    common_users = all_comms[all_comms > 5].index.values
    ##In common users for source and dest, or a purchase or meeting
    print(temp_df.shape)
    temp_df = temp_df[((temp_df['Source'].isin(common_users)) & 
                      (temp_df['Destination'].isin(common_users))) |
                      (temp_df['Etype'] == 2) | 
                      (temp_df['Etype'] == 3) ]
    print(temp_df.shape)
    
    describe_network_interactions(temp_df, purchase_row, output_dict,
                                  suspicious_indicator)
    
def time_filter_df(input_df, time_forward, time_backward, purchase_time):
    filtered_df = input_df.copy()
    start_time = purchase_time - time_backward
    stop_time  = purchase_time + time_forward
    filtered_df = filtered_df[(filtered_df['TimeStamp'] > start_time) &
                      (filtered_df['TimeStamp'] < stop_time)]
    return filtered_df
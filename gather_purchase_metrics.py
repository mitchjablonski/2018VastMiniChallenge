# -*- coding: utf-8 -*-
"""
Created on Mon Aug 13 21:41:48 2018

@author: mitch
"""
import math
import pandas as pd
import numpy as np
import add_names_to_df as get_names_from_company_index

'''
0 is for calls
1 is for emails
2 is for purchases
3 is for meetings
'''

def purchase_analysis(purchase_row, input_df, layers, 
                      output_dict, suspicious_indicator, filename, replace_dict):
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
    record_purchase_informat(filename, filtered_df, replace_dict)

def record_purchase_information(filename, data_frame, replace_dict):
    data_frame = data_frame.copy()
    data_frame['Etype'].replace(replace_dict, inplace=True)
    data_frame = get_names_from_company_index.add_names_to_data_frame(data_frame)
    data_frame.to_csv(filename)
    return data_frame

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
    
def determine_layers_out(input_df, purchase_row, output_dict, suspicious_indicator):
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
    temp_df.sort_values(by='TimeStamp', inplace=True)
    describe_network_interactions(temp_df, purchase_row, output_dict, suspicious_indicator)
    
    print(layers)
    print('Forcing layers to be 1')
    layers = 1
    return layers

def look_at_size_of_network_X_layers_out(input_df, purchase_row, layers, output_dict, suspicious_indicator):
    temp_layers = 0 
    output_df = input_df.copy()
    
    source_dest = (output_df['Source'].isin([purchase_row['Source']])) | (output_df['Destination'].isin([purchase_row['Destination']]))
    dest_source = (output_df['Source'].isin([purchase_row['Destination']])) | (output_df['Destination'].isin([purchase_row['Source']]))
    
    temp_df = output_df[source_dest | dest_source]
    
    first_meeting_count = temp_df[temp_df['Etype'] == 3]['Etype'].count()
    print('Meetings post first filter {}'.format(first_meeting_count))
    output_dict['first_meeting_count'].append(first_meeting_count)
    
    temp_df[temp_df['Etype'] == 3].to_csv('purchase_communication_results/first_pass_meeting_info_{}_{}_{}'
                                    .format(purchase_row['TimeStamp'], 
                                            purchase_row['Source'], 
                                            purchase_row['Destination']))
    while temp_layers < layers:
        source_dest = (output_df['Source'].isin(temp_df['Source'])) | (output_df['Destination'].isin(temp_df['Destination']))
        dest_source = (output_df['Source'].isin(temp_df['Destination'])) | (output_df['Destination'].isin(temp_df['Source']))
        temp_df = output_df[source_dest | dest_source]
        #temp_df = output_df[source_dest]
        temp_layers += 1
        
    temp_df = get_names_from_company_index.add_names_to_data_frame(temp_df)
    temp_dest = temp_df['Destination_Names'].value_counts()
    temp_source = temp_df['Source_Names'].value_counts()
    all_comms = temp_source.add(temp_dest, fill_value = 0)
    top_ten_comms = all_comms.sort_values(ascending=False)[:10]
    common_users = all_comms[all_comms > 1].index.values
    ##In common users for source and dest, or a purchase or meeting
    
    print(temp_df.shape)
    temp_df = temp_df[((temp_df['Source_Names'].isin(common_users)) & 
                      (temp_df['Destination_Names'].isin(common_users))) |
                      (temp_df['Etype'] == 2) | 
                      (temp_df['Etype'] == 3) ]
    print(temp_df.shape)
    
    second_meeting_count = temp_df[temp_df['Etype'] == 3]['Etype'].count()
    
    print('Meetings post second filter {}'.format(second_meeting_count))
    output_dict['second_meeting_count'].append(second_meeting_count)
    
    temp_df[temp_df['Etype'] == 3].to_csv('purchase_communication_results/second_pass_meeting_info_{}_{}_{}'
                                    .format(purchase_row['TimeStamp'], 
                                            purchase_row['Source'], 
                                            purchase_row['Destination']))
    print(top_ten_comms)
    
    
    top_ten_comms.to_csv('purchase_communication_results/top_ten_comms_{}_{}_{}.csv'.format(purchase_row['TimeStamp'], 
                                                             purchase_row['Source'], 
                                                             purchase_row['Destination']))
    
    temp_df.sort_values(by='TimeStamp', inplace=True)
    describe_network_interactions(temp_df, purchase_row, output_dict,
                                  suspicious_indicator)
    
def time_filter_df(input_df, time_forward, time_backward, purchase_time):
    filtered_df = input_df.copy()
    start_time = purchase_time - time_backward
    stop_time  = purchase_time + time_forward
    filtered_df = filtered_df[(filtered_df['TimeStamp'] > start_time) &
                      (filtered_df['TimeStamp'] < stop_time)]
    return filtered_df

def describe_network_interactions(temp_df, purchase_row, output_dict, suspicious_indicator):
    row, columns = temp_df.shape
    new_df = temp_df.set_index('full_date')
    
    rolling_window_counts = new_df.rolling(window='8D', min_periods=1)['TimeStamp'].count()
    primary_source_int = temp_df[(temp_df['Destination'] == purchase_row['Source']) | 
                                    (temp_df['Source'] == purchase_row['Source'])]['Source'].count()
    
    temp_df['full_date'] = pd.to_datetime(temp_df['full_date'])
    temp_df.sort_values(by='full_date', inplace=True)
    time_between_interactions  = temp_df['full_date'].diff()
    rolling_diff  = temp_df['full_date'].diff(periods=5)

    unique_source_dest = np.concatenate((temp_df['Source'].unique(), temp_df['Destination'].unique()))
    unique_source_dest = np.unique(unique_source_dest)
    
    primary_dest_int = temp_df[(temp_df['Destination'] == purchase_row['Destination']) | 
                                (temp_df['Source'] == purchase_row['Destination'])]['Destination'].count()

    mean_dest = temp_df['Destination'].value_counts().mean()
    mean_source = temp_df['Source'].value_counts().mean()
    unique_source = temp_df['Source'].nunique()
    unique_dest = temp_df['Destination'].nunique()
    tot_entries = temp_df['Source'].count()
    
    output_dict['Source'].append(purchase_row['Source'])
    output_dict['Destination'].append(purchase_row['Destination'])
    output_dict['Source_Name'].append(purchase_row['Source_Name'])
    output_dict['Destination_Name'].append(purchase_row['Destination_Name'])
    output_dict['Primary_source_interactions'].append(primary_source_int)
    output_dict['Primary_dest_interactions'].append(primary_dest_int)
    output_dict['Avg_dest_count'].append(mean_dest)
    output_dict['Avg_source_count'].append(mean_source)
    output_dict['num_uniq_source'].append(unique_source)
    output_dict['num_uniq_dest'].append(unique_dest)
    output_dict['combined_unique'].append(unique_source_dest.size)
    output_dict['total_entires'].append(tot_entries)
    output_dict['suspicious_indicator'].append(suspicious_indicator)
    output_dict['mean_time_btwn'].append(time_between_interactions.mean())
    output_dict['max_time_btwn'].append(time_between_interactions.max())
    output_dict['min_time_btwn'].append(time_between_interactions.min())
    output_dict['mean_rolling_diff'].append(rolling_diff.mean())
    output_dict['max_rolling_diff'].append(rolling_diff.max())
    output_dict['min_rolling_diff'].append(rolling_diff.min())
    output_dict['mean_rolling_window'].append(rolling_window_counts.mean())
    output_dict['max_rolling_window'].append(rolling_window_counts.max())
    output_dict['min_rolling_window'].append(rolling_window_counts.min())
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
def time_filter_one_layer(purchase_row, input_df, filename, replace_dict):
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
    filtered_df = filtered_df.loc[source_dest | dest_source]
    
    ##TODO should this go out another layer?  is there more info there?
    filtered_df = filtered_df.append(purchase_row)
    filtered_df = get_names_from_company_index.add_names_to_data_frame(filtered_df)
    record_purchase_information(filename, filtered_df, replace_dict)
    return filtered_df

def time_filter_df(input_df, time_forward, time_backward, purchase_time):
    filtered_df = input_df.copy()
    start_time = purchase_time - time_backward
    stop_time  = purchase_time + time_forward
    filtered_df = filtered_df.loc[(filtered_df['TimeStamp'] > start_time) &
                      (filtered_df['TimeStamp'] < stop_time)]
    return filtered_df

def purchase_analysis(purchase_row, input_df, layers, 
                      output_dict, suspicious_indicator,
                      filename, replace_dict, unique_mtg_attendees,
                      analysis_type):
    ##We will want to get all of their interactions that occured within a one month timeframe
    network_df = look_at_size_of_network_X_layers_out(input_df, purchase_row, layers, 
                                         output_dict, suspicious_indicator, 
                                         unique_mtg_attendees,
                                         analysis_type)

    record_purchase_information(filename, network_df, replace_dict)
    
    return network_df

def record_purchase_information(filename, data_frame, replace_dict):
    data_frame = data_frame.copy()
    data_frame['Etype'].replace(replace_dict, inplace=True)
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
    temp_df = output_df.loc[source_dest | dest_source]
    #temp_df = output_df[source_dest]
    print('determine_layers_out')
    layers = 0
    full_cols, _ = output_df.shape
    temp_cols, _ = temp_df.shape
    while temp_cols < full_cols:
        source_dest = (output_df['Source'].isin(temp_df['Source'])) | (output_df['Destination'].isin(temp_df['Destination']))
        dest_source = (output_df['Source'].isin(temp_df['Destination'])) | (output_df['Destination'].isin(temp_df['Source']))
        temp_df = output_df.loc[source_dest | dest_source]
        #temp_df = output_df[source_dest]
        layers += 1
        temp_cols, _ = temp_df.shape
    temp_df.sort_values(by='TimeStamp', inplace=True)
    describe_network_interactions(temp_df, purchase_row, output_dict, suspicious_indicator)
    unique_mtg_attendees = np.concatenate((temp_df['Source'].unique(), temp_df['Destination'].unique()))
    unique_mtg_attendees = np.unique(unique_mtg_attendees)
    print(layers)
    #print('Forcing layers to be 1')
    #layers = 1
    return layers, unique_mtg_attendees

def get_communications_counts(input_df):
    temp_df = input_df.copy()
    temp_dest = temp_df['Destination_Names'].value_counts()
    temp_source = temp_df['Source_Names'].value_counts()
    all_comms = temp_source.add(temp_dest, fill_value = 0)
    all_comms = all_comms.sort_values(ascending=False)
    return all_comms

def apply_common_user_or_etype_rule(input_df):
    #In common users for source or dest, or it is a meeting
    ##Drop anyone with less than 1 comm not in source and dest?
    ##We need our source and destination to appear more than once, or have it be 
    #an instance of a meeting
    temp_df = input_df.copy()
    all_comms_pre_filter = get_communications_counts(temp_df)
    ##We want people who are communicating in our group, but we want to avoid 
    ##Our group exploding due to a well connected person, we might have one meeting
    ##With a very suspicious person, but that person may be too well connect
    ##Thus corrupting our dat
    common_users = all_comms_pre_filter[(all_comms_pre_filter > 2) & (all_comms_pre_filter < 3000)].index.values
    common_user_filter = ((temp_df['Source_Names'].isin(common_users)) &
                      (temp_df['Destination_Names'].isin(common_users)))
    
    common_meeting_filter = (temp_df['Etype'] == 3) & (temp_df['Source_Names'].isin(common_users) | 
                                                    temp_df['Destination_Names'].isin(common_users))
    
    temp_df = temp_df.loc[common_user_filter | common_meeting_filter]
    
    all_comms_post_filter = get_communications_counts(temp_df)
    
    return temp_df, all_comms_pre_filter, all_comms_post_filter
    
def log_communication_network(input_df, analysis_type, timestamp, 
                              source_name, dest_name, data_layer):
    input_df.to_csv(
            'purchase_communication_results/{}_group_structure_for_{}_{}_{}_layer_{}.csv'
            .format(analysis_type,
                    timestamp, 
                    source_name, 
                    dest_name,
                    data_layer))

def look_at_size_of_network_X_layers_out(input_df, purchase_row, layers, output_dict, 
                                         suspicious_indicator, unique_mtg_attendees,
                                         analysis_type):
    temp_layers = 0 
    seconds_in_a_month = int(2.628e+6)
    purchase_row = purchase_row.copy()
    output_df = input_df.copy()
    output_df = time_filter_df(output_df, seconds_in_a_month*6, seconds_in_a_month*12, purchase_row['TimeStamp'])
    source_dest = (output_df['Source'].isin([purchase_row['Source']])) | (output_df['Destination'].isin([purchase_row['Destination']]))
    dest_source = (output_df['Source'].isin([purchase_row['Destination']])) | (output_df['Destination'].isin([purchase_row['Source']]))
    
    temp_df = output_df.loc[source_dest | dest_source]
    print('Making original purchase Etype 5')
    purchase_row['Etype'] = 5

    ##We have either communicated with this person more than once, or we communicated with them in a meeting.
    temp_df, all_comms_pre_filter, all_comms_post_filter = apply_common_user_or_etype_rule(temp_df)
    temp_df = temp_df.append(purchase_row)
    
    log_communication_network(temp_df, analysis_type, purchase_row['TimeStamp'], 
                              purchase_row['Source_Names'], purchase_row['Destination_Names'], (temp_layers+1))
    
    if temp_df.loc[temp_df['Etype'] == 3]['Source'].count() > 0:
        print('Meeting Found on first pass for Source {} Dest {} at time {}'.format(
                purchase_row['Source_Names'],                                                                   
                purchase_row['Destination_Names'],
                purchase_row['TimeStamp']))
    
    while temp_layers < layers:
        source_dest = (output_df['Source'].isin(temp_df['Source'])) | (output_df['Destination'].isin(temp_df['Destination']))
        dest_source = (output_df['Source'].isin(temp_df['Destination'])) | (output_df['Destination'].isin(temp_df['Source']))
        temp_df = output_df.loc[source_dest | dest_source]
        temp_df, all_comms_pre_filter, all_comms_post_filter = apply_common_user_or_etype_rule(temp_df)
        temp_layers += 1
        temp_df = temp_df.append(purchase_row)
        log_communication_network(temp_df, analysis_type, purchase_row['TimeStamp'], 
                              purchase_row['Source_Names'], purchase_row['Destination_Names'], (temp_layers+1))
        
    
    
    meeting_df = temp_df.loc[temp_df['Etype'] == 3]
    found_unique_meeting_repeat = np.any(meeting_df['Source'].isin(unique_mtg_attendees) | 
                                            meeting_df['Destination'].isin(unique_mtg_attendees)) 
    
    
    meeting_df.to_csv('purchase_communication_results/{}_meeting_info_{}_{}_{}.csv'
                      .format(analysis_type,
                              purchase_row['TimeStamp'], 
                              purchase_row['Source_Names'], 
                              purchase_row['Destination_Names']))
        
    all_comms_pre_filter.to_csv('purchase_communication_results/{}all_communicators_pre_filter_{}_{}_{}.csv'.format(analysis_type,
                                                                                             purchase_row['TimeStamp'],         
                                                                                             purchase_row['Source_Names'], 
                                                                                             purchase_row['Destination_Names']))
    
    all_comms_post_filter.to_csv('purchase_communication_results/{}all_communicators_post_filter_{}_{}_{}.csv'.format(analysis_type,
                                                                                             purchase_row['TimeStamp'],         
                                                                                             purchase_row['Source_Names'], 
                                                                                             purchase_row['Destination_Names']))
    
    print('Meetings found {}'.format(meeting_df['Etype'].count()))
    output_dict['meeting_count'].append(meeting_df['Etype'].count())
    output_dict['found_unique_meeting_repeat'].append(found_unique_meeting_repeat)
    
    

    temp_df.sort_values(by='TimeStamp', inplace=True)
    
    describe_network_interactions(temp_df, purchase_row, output_dict,
                                  suspicious_indicator)
    
    return temp_df
    

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
    
    primary_dest_int = temp_df.loc[(temp_df['Destination'] == purchase_row['Destination']) | 
                                (temp_df['Source'] == purchase_row['Destination'])]['Destination'].count()

    mean_dest = temp_df['Destination'].value_counts().mean()
    mean_source = temp_df['Source'].value_counts().mean()
    unique_source = temp_df['Source'].nunique()
    unique_dest = temp_df['Destination'].nunique()
    tot_entries = temp_df['Source'].count()
    
    output_dict['Source'].append(purchase_row['Source'])
    output_dict['Destination'].append(purchase_row['Destination'])
    output_dict['Source_Names'].append(purchase_row['Source_Names'])
    output_dict['Destination_Names'].append(purchase_row['Destination_Names'])
    output_dict['Primary_source_interactions'].append(primary_source_int)
    output_dict['Primary_dest_interactions'].append(primary_dest_int)
    output_dict['Avg_dest_count'].append(mean_dest)
    output_dict['Avg_source_count'].append(mean_source)
    output_dict['num_uniq_source'].append(unique_source)
    output_dict['num_uniq_dest'].append(unique_dest)
    output_dict['combined_unique'].append(unique_source_dest.size)
    output_dict['total_entries'].append(tot_entries)
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
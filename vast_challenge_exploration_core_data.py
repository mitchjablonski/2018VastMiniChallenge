# -*- coding: utf-8 -*-
"""
Created on Mon Jul 23 19:28:48 2018

@author: mitch
"""

from __future__ import division
from __future__ import print_function

import pandas as pd
import date_time_conversion as dt_converter
import high_level_investigation as high_level_investigation
import math
import add_names_to_df as get_names_from_company_index
import networkx as nx



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
    
def load_data(use_preprocess, data_type, columns):
    if use_preprocess:
        curr_data = pd.read_csv('date_timed_{}.csv'.format(data_type))
    else:
        curr_data = pd.read_csv('{}.csv'.format(data_type), header=None, names = columns)
        curr_data = dt_converter.convert_time(curr_data)
        curr_data.to_csv('date_timed_{}.csv'.format(data_type))
    return curr_data

def get_gail_id (purchase_data):
    return purchase_data['Destination'].value_counts(ascending=False).index[0]

def compare_purchases_for_gail(use_preprocess, columns):
    data_type = 'purchases'
    purchase_data = load_data(use_preprocess, data_type, columns)
    purchases_described = pd.DataFrame()
    purchase_data = get_names_from_company_index.add_names_to_data_frame(purchase_data)
    gail_id = get_gail_id(purchase_data)
    data = 'gail'
    data_types =['gail', 'no_gail']
    for data in data_types:
        if data is 'gail':
            temp_data = purchase_data[gail_id == purchase_data['Destination']]
        else:
            temp_data = purchase_data[gail_id != purchase_data['Destination']]
        purchases_described = purchases_described.append(high_level_investigation.investigate_data(temp_data, data), 
                                                     ignore_index=True, sort=True)
    
    purchases_described['date_time'] = pd.to_datetime(purchases_described['month_yr'])
    purchases_described.sort_values(by='date_time',inplace=True)
    purchases_described.to_csv('purchases_described_comparing_gail.csv')
    return purchases_described

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
    
def determine_layers_out(input_df, purchase_row):
    output_df = input_df.copy()    
    source_dest = (output_df['Source'].isin([purchase_row['Source']])) | (output_df['Destination'].isin([purchase_row['Destination']]))
    #dest_source = (output_df['Source'].isin([purchase_row['Destination']])) | (output_df['Destination'].isin([purchase_row['Source']]))
    #temp_df = output_df[source_dest | dest_source]
    temp_df = output_df[source_dest]
    print('determine_layers_out')
    layers = 0
    full_cols, _ = output_df.shape
    temp_cols, _ = temp_df.shape
    while temp_cols < full_cols:
        source_dest = (output_df['Source'].isin(temp_df['Source'])) | (output_df['Destination'].isin(temp_df['Destination']))
        #dest_source = (output_df['Source'].isin(temp_df['Destination'])) | (output_df['Destination'].isin(temp_df['Source']))
        #temp_df = output_df[source_dest | dest_source]
        temp_df = output_df[source_dest]
        layers += 1
        temp_cols, _ = temp_df.shape
    
    print(layers)
    print('Number unique Sources {}'.format(temp_df['Source'].nunique()))
    print('Number Unique Destination {}'.format(temp_df['Destination'].nunique()))
    print('Total Number of entries {}'.format(temp_df['Source'].count()))
    print('Forcing layers to be 1')
    layers=1
    return layers

def look_at_size_of_network_X_layers_out(input_df, purchase_row, layers):
    temp_layers = 0 
    output_df = input_df.copy()
    ##TODO is time limiting good here?
    '''
    seconds_in_month = int(2.628e+6)
    #seconds_in_five_days = int(432000)
    time_window = seconds_in_month
    start_time = purchase_row['TimeStamp'] - time_window
    stop_time  = purchase_row['TimeStamp'] + time_window
    output_df = output_df[(output_df['TimeStamp'] > start_time) &
                      (output_df['TimeStamp'] < stop_time)]
    '''
    ##TODO
    source_dest = (output_df['Source'].isin([purchase_row['Source']])) | (output_df['Destination'].isin([purchase_row['Destination']]))
    #dest_source = (output_df['Source'].isin([purchase_row['Destination']])) | (output_df['Destination'].isin([purchase_row['Source']]))
    #temp_df = output_df[source_dest | dest_source]
    temp_df = output_df[source_dest]
    while temp_layers < layers:
        source_dest = (output_df['Source'].isin(temp_df['Source'])) | (output_df['Destination'].isin(temp_df['Destination']))
        if layers == 1:
            source_dest = (source_dest | (output_df['Destination'].isin([purchase_row['Source']])) | 
                                        (output_df['Source'].isin([purchase_row['Destination']])))
        #dest_source = (output_df['Source'].isin(temp_df['Destination'])) | (output_df['Destination'].isin(temp_df['Source']))
        #temp_df = output_df[source_dest | dest_source]
        temp_df = output_df[source_dest]
        temp_layers += 1
        print(layers)
    print('Number unique Sources {}'.format(temp_df['Source'].nunique()))
    print('Number Unique Destination {}'.format(temp_df['Destination'].nunique()))
    print('Total Number of entries {}'.format(temp_df['Source'].count()))

def purchase_analysis(purchase_row, input_df, layers):
    ##We will want to get all of their interactions that occured within a one month timeframe
    look_at_size_of_network_X_layers_out(input_df, purchase_row, layers)
    source = purchase_row['Source']
    destination = purchase_row['Destination']
    purchase_time = purchase_row['TimeStamp']
    seconds_in_month = int(2.628e+6)
    #seconds_in_five_days = int(432000)
    time_window = seconds_in_month
    start_time = purchase_time - time_window
    stop_time  = purchase_time + time_window
    filtered_df = input_df[(input_df['TimeStamp'] > start_time) &
                      (input_df['TimeStamp'] < stop_time)]
    ##Our destination in the suspicous purchase, is actaully the only interaction
    #they have in the suspicious dataset, we should potentially expect the purchasing agent(destination)
    #to have little contact with the soruce
    source_dest = (filtered_df['Source'] == source) | (filtered_df['Destination'] == destination)
    dest_source = (filtered_df['Source'] == destination) | (filtered_df['Destination'] == source)
    filtered_df = filtered_df[source_dest | dest_source]
    
    ##TODO should this go out another layer?  is there more info there?
    filtered_df = filtered_df.append(purchase_row)
    
    return filtered_df
    
def analyze_confirmed_suspicious(columns, replace_dict, main_df, build_network_graph):
    sus_purchase = pd.read_csv('Suspicious_purchases.csv', names=columns)
    sus_purchase = dt_converter.convert_time(sus_purchase)
    sus_purchase_row = sus_purchase.iloc[0]
    sus_purchase.to_csv('sus_purchase_date_time_conv.csv')
    email_data = pd.read_csv(r'Suspicious_emails.csv', header=None, names = columns)
    call_data  =  pd.read_csv(r'Suspicious_calls.csv', header=None, names = columns)
    meeting_data =  pd.read_csv(r'Suspicious_meetings.csv', header=None, names = columns)
    sus_df = pd.DataFrame()
    sus_df = sus_df.append([email_data, call_data, meeting_data])
    sus_df = dt_converter.convert_time(sus_df)
    sus_df.sort_values('full_date', inplace=True)
    layers = determine_layers_out(sus_df, sus_purchase_row)
    sus_analysis_df = purchase_analysis(sus_purchase_row, sus_df, layers)
    
    
    filename = 'suspected_suspicious/confirmed_suspicious.csv'
    sus_analysis_df = record_purchase_information(filename, sus_analysis_df, replace_dict)
    if build_network_graph:
        perform_network_analysis(sus_analysis_df)

    print('full_df')
    sus_analysis_full_df = purchase_analysis(sus_purchase_row, main_df, layers)
    filename = 'suspected_suspicious/confirmed_suspicious_full_set.csv'
    sus_analysis_full_df = record_purchase_information(filename, sus_analysis_full_df, replace_dict)
    
    if build_network_graph:
        perform_network_analysis(sus_analysis_full_df)
    print(determine_metrics_for_purchase(sus_analysis_full_df))
    
    print('only_sus_df')
    return determine_metrics_for_purchase(sus_analysis_df), layers

def record_purchase_information(filename, data_frame, replace_dict):
    data_frame = data_frame.copy()
    data_frame['Etype'].replace(replace_dict, inplace=True)
    data_frame = get_names_from_company_index.add_names_to_data_frame(data_frame)
    data_frame.to_csv(filename)
    return data_frame

def analyze_suspected_suspicious(main_df, replace_dict, layers, build_network_graph):
    other_suspicious = pd.read_csv('Other_suspicious_purchases.csv')
    other_suspicious.rename(columns={'Dest':'Destination','Time':'TimeStamp'}, inplace=True)
    other_suspicious = dt_converter.convert_time(other_suspicious)
    for index, rows in other_suspicious.iterrows():
        analysis_df = purchase_analysis(rows, main_df, layers)
        filename = ('suspected_suspicious/suspected_suspicous_{}_{}_{}.csv'.format(rows['Source'], 
                                                                                   rows['Destination'],
                                                                                   rows['TimeStamp']))
        analysis_df = record_purchase_information(filename, analysis_df, replace_dict)
        if build_network_graph:
            perform_network_analysis(analysis_df)
        print(determine_metrics_for_purchase(analysis_df))

def analyze_all_purchases(main_df, replace_dict, purchase_df, layers, build_network_graph):
    purchase_df.reset_index(inplace=True)
    print('In all purchase region')
    for index, rows in purchase_df.iterrows():
        #analysis_df = purchase_analysis(rows, main_df)
        if (index % 100) == 1:
            print('index {}'.format(index))
        if index >6000 and index < 6012:
            analysis_df = purchase_analysis(rows, main_df, layers)
            filename = ('regular_purchases/regular_purchases_{}_{}_{}.csv'.format(rows['Source'], 
                                                                                  rows['Destination'],
                                                                                  rows['TimeStamp']))
            analysis_df = record_purchase_information(filename, analysis_df, replace_dict)
            print(determine_metrics_for_purchase(analysis_df))
            ##Most have way more unique sources, destinations, and total interactions.
            ##Most peoples time delta is significantly less, IE  < 1hr?
            
            ##Should consider taking a look at the amount of purchases our suspicious folks have made
            '''
            (unique_sources, unique_destinations, 
            total_interactions, int_per_source, 
            int_per_dest, mean_time_between)
            '''
            if build_network_graph:
                perform_network_analysis(analysis_df)
            
def perform_network_analysis(input_df):
    FG = nx.from_pandas_edgelist(input_df, source='Source_Names', target='Destination_Names', edge_attr=True,)
    nx.draw_networkx(FG, with_labels=True)
    degree_cent = (nx.algorithms.degree_centrality(FG))
    density =(nx.density(FG))
    avg_shortest = (nx.average_shortest_path_length(FG))
    avg_degree_connectivity = (nx.average_degree_connectivity(FG))
    
    # Next, use nx.connected_components to get the list of components,
    # then use the max() command to find the largest one:
    components = nx.connected_components(FG)
    largest_component = max(components, key=len)
    subgraph = FG.subgraph(largest_component)
    diameter = nx.diameter(subgraph)
    print("Network diameter of largest component:", diameter)
    triadic_closure = nx.transitivity(FG)
    print("Triadic closure:", triadic_closure)
    
    print(degree_cent, density, avg_shortest, avg_degree_connectivity)

def remove_gail_from_df(gail_id, input_df):
    return_df = input_df.copy()
    return  return_df[(return_df['Source'] != gail_id) & 
                          (return_df['Destination'] != gail_id)]
def perform_deep_purchase_analysis(columns, replace_dict, build_network_graph):
    main_df = pd.DataFrame()
    for data in data_types:
        print(data)
        curr_data = load_data(use_preprocess, data, columns)
        if data == 'purchases':
            gail_id = get_gail_id(curr_data)
            ##Too many purchases to look at gail, drop all including gail
            full_purchase_df = curr_data
            purchase_df = remove_gail_from_df(gail_id, full_purchase_df)
        else:
            main_df = main_df.append(curr_data, ignore_index = True)
            
    (unique_sources, unique_destinations, 
     total_interactions, int_per_source, 
     int_per_dest, mean_time_between), layers = analyze_confirmed_suspicious(columns, replace_dict, 
                                                                             main_df, build_network_graph)
    
    print(unique_sources, unique_destinations, 
          total_interactions, int_per_source, 
          int_per_dest, mean_time_between)
    
    analyze_suspected_suspicious(main_df, replace_dict, layers, build_network_graph)
    analyze_all_purchases(main_df, replace_dict, purchase_df, layers, build_network_graph)

def _main(columns, data_types, replace_dict,
          use_preprocess, deep_purchase_analysis, 
          data_describe_processing, compare_purchase_gail,
          build_network_graph):
    
    described_data = pd.DataFrame()
    purchases_described = pd.DataFrame()
    
    if data_describe_processing:
        for data in data_types:
            print(data)
            curr_data = load_data(use_preprocess, data, columns)
            described_data = described_data.append(high_level_investigation.investigate_data(curr_data,data), ignore_index=True, sort=True)
        described_data['date_time'] = pd.to_datetime(described_data['month_yr'])
        described_data.sort_values(by='date_time',inplace=True)
        described_data.to_csv('data_described_frequency_v2.csv')
        
    if compare_purchase_gail:
        purchases_described = compare_purchases_for_gail(use_preprocess, columns)
        
    if deep_purchase_analysis:
        perform_deep_purchase_analysis(columns, replace_dict, build_network_graph)
    return described_data, purchases_described
    
#Data is over a time period of 2.5yrs
if __name__ == '__main__':
    columns = ['Source', 'Etype', 'Destination', 'TimeStamp']
    data_types = ['calls', 'emails', 'purchases', 'meetings']
    #data_types = ['meetings']
    replace_dict = {0:'calls',1:'emails',2:'purchases',3:'meetings'}
    use_preprocess = True
    deep_purchase_analysis = True
    data_describe_processing = False
    compare_purchase_gail = False
    build_network_graph = False
    described_data, purchases_described = _main(columns, data_types, replace_dict,
                                                use_preprocess, deep_purchase_analysis, 
                                                data_describe_processing, compare_purchase_gail,
                                                build_network_graph)
        
    
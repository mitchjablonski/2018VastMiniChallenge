# -*- coding: utf-8 -*-
"""
Created on Mon Jul 23 19:28:48 2018

@author: mitch
"""

from __future__ import division
from __future__ import print_function

import pandas as pd
import networkx as nx
from collections import defaultdict

import date_time_conversion as dt_converter
import high_level_investigation as high_level_investigation
import add_names_to_df as get_names_from_company_index
import gather_purchase_metrics as gather_purchase_metrics



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
    
def analyze_confirmed_suspicious(columns, replace_dict, main_df, build_network_graph, output_dict):
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
    
    suspicious_conf = 1
    layers = gather_purchase_metrics.determine_layers_out(sus_df, sus_purchase_row, output_dict, suspicious_conf)
    
    sus_analysis_df = gather_purchase_metrics.purchase_analysis(sus_purchase_row, sus_df, layers, output_dict, suspicious_conf)
    
    gather_purchase_metrics.compare_confirmed_suspicious_in_larger_data_set(main_df, sus_df, sus_purchase_row)
    
    filename = 'suspected_suspicious/confirmed_suspicious.csv'
    sus_analysis_df = record_purchase_information(filename, sus_analysis_df, replace_dict)
    if build_network_graph:
        perform_network_analysis(sus_analysis_df)

    print('full_df')
    sus_analysis_full_df = gather_purchase_metrics.purchase_analysis(sus_purchase_row, main_df, layers, output_dict, suspicious_conf)
    filename = 'suspected_suspicious/confirmed_suspicious_full_set.csv'
    sus_analysis_full_df = record_purchase_information(filename, sus_analysis_full_df, replace_dict)
    
    if build_network_graph:
        perform_network_analysis(sus_analysis_full_df)
    #print(determine_metrics_for_purchase(sus_analysis_full_df))
    
    print('only_sus_df')
    #return determine_metrics_for_purchase(sus_analysis_df), layers
    return layers

def record_purchase_information(filename, data_frame, replace_dict):
    data_frame = data_frame.copy()
    data_frame['Etype'].replace(replace_dict, inplace=True)
    data_frame = get_names_from_company_index.add_names_to_data_frame(data_frame)
    data_frame.to_csv(filename)
    return data_frame

def analyze_suspected_suspicious(main_df, replace_dict, layers, build_network_graph, output_dict):
    other_suspicious = pd.read_csv('Other_suspicious_purchases.csv')
    other_suspicious.rename(columns={'Dest':'Destination','Time':'TimeStamp'}, inplace=True)
    other_suspicious = dt_converter.convert_time(other_suspicious)
    suspicious_conf = 1
    for index, rows in other_suspicious.iterrows():
        analysis_df = gather_purchase_metrics.purchase_analysis(rows, main_df, layers, output_dict, suspicious_conf)
        filename = ('suspected_suspicious/suspected_suspicous_{}_{}_{}.csv'.format(rows['Source'], 
                                                                                   rows['Destination'],
                                                                                   rows['TimeStamp']))
        analysis_df = record_purchase_information(filename, analysis_df, replace_dict)
        if build_network_graph:
            perform_network_analysis(analysis_df)
        #print(determine_metrics_for_purchase(analysis_df))

def analyze_all_purchases(main_df, replace_dict, purchase_df, layers, build_network_graph, output_dict):
    purchase_df.reset_index(inplace=True)
    print('In all purchase region, not logging all runs on')
    suspicious_conf = -1
    for index, rows in purchase_df.iterrows():
        #analysis_df = purchase_analysis(rows, main_df)
        if (index % 1000) == 1:
            print('index {}'.format(index))
        #if index >7000 and index < 7012:
        if True:
            analysis_df = gather_purchase_metrics.purchase_analysis(rows, main_df, layers, output_dict, suspicious_conf)
            #Not logging
            '''
            filename = ('regular_purchases/regular_purchases_{}_{}_{}.csv'.format(rows['Source'], 
                                                                                  rows['Destination'],
                                                                                  rows['TimeStamp']))
            analysis_df = record_purchase_information(filename, analysis_df, replace_dict)
            '''
            #print(determine_metrics_for_purchase(analysis_df))

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
    
    output_dict = defaultdict(list)
    main_df['full_date'] = pd.to_datetime(main_df['full_date'])
    main_df = main_df.sort_values(by='full_date')
    
    layers = analyze_confirmed_suspicious(columns, replace_dict, 
                                 main_df, build_network_graph,
                                 output_dict)
    analyze_suspected_suspicious(main_df, replace_dict, layers, build_network_graph, output_dict)
    analyze_all_purchases(main_df, replace_dict, purchase_df, layers, build_network_graph, output_dict)
    
    result_df = pd.DataFrame.from_dict(output_dict)
    result_df.to_csv('deep_purchase_analysis_result_df.csv')

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
        
    
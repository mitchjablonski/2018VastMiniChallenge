# -*- coding: utf-8 -*-
"""
Created on Mon Jul 23 19:28:48 2018

@author: mitch
"""

from __future__ import division
from __future__ import print_function

import pandas as pd
import numpy as np
from collections import defaultdict

import date_time_conversion as dt_converter
import high_level_investigation as high_level_investigation
import add_names_to_df as get_names_from_company_index
import analyze_suspicious_purchases as analyze_suspicious_purchases



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
            temp_data = purchase_data.loc[gail_id == purchase_data['Destination']]
        else:
            temp_data = purchase_data.loc[gail_id != purchase_data['Destination']]
        purchases_described = purchases_described.append(high_level_investigation.investigate_data(temp_data, data), 
                                                     ignore_index=True, sort=True)
    
    purchases_described['date_time'] = pd.to_datetime(purchases_described['month_yr'])
    purchases_described.sort_values(by='date_time',inplace=True)
    purchases_described.to_csv('purchases_described_comparing_gail.csv')
    return purchases_described

def remove_gail_from_df(gail_id, input_df):
    return_df = input_df.copy()
    return  return_df.loc[(return_df['Source'] != gail_id) & 
                          (return_df['Destination'] != gail_id)]
    
def perform_deep_purchase_analysis(columns, replace_dict, build_network_graph, analyze_full_dataset):
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
    
    layers, unique_mtg_attendees = analyze_suspicious_purchases.analyze_confirmed_suspicious(columns, replace_dict, 
                                 main_df, build_network_graph,
                                 output_dict)
    
    analyze_suspicious_purchases.analyze_suspected_suspicious(main_df, replace_dict, layers, 
                                                              build_network_graph, output_dict, 
                                                              unique_mtg_attendees)
    
    if analyze_full_dataset:
        analyze_suspicious_purchases.analyze_all_purchases(main_df, replace_dict, purchase_df, 
                                                           layers, build_network_graph, output_dict,
                                                           unique_mtg_attendees)
    
    result_df = pd.DataFrame.from_dict(output_dict, orient='index').transpose()
    result_df.to_csv('purchase_communication_results/deep_purchase_analysis_result_df.csv')

def perform_meeting_analysis(use_preprocess, columns):
    data = 'meetings'
    meeting_data = load_data(use_preprocess, data, columns)
    unique_source_dest = np.concatenate((meeting_data['Source'].unique(), meeting_data['Destination'].unique()))
    unique_source_dest = np.unique(unique_source_dest)
    meeting_data = get_names_from_company_index.add_names_to_data_frame(meeting_data)
    temp_dest = meeting_data['Destination_Names'].value_counts()
    temp_source = meeting_data['Source_Names'].value_counts()
    all_comms = temp_source.add(temp_dest, fill_value = 0)
    meeting_attendees = all_comms.sort_values(ascending=False)
    temp_dest.to_csv('meeting_results/meeting_destination_with_counts.csv')
    temp_source.to_csv('meeting_results/meeting_source_with_counts.csv')
    meeting_attendees.to_csv('meeting_results/meeting_attendees_with_counts.csv')
    

def _main(columns, data_types, replace_dict,
          use_preprocess, deep_purchase_analysis, 
          data_describe_processing, compare_purchase_gail,
          build_network_graph, analyze_full_dataset, analyze_meeting_data):
    
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
        perform_deep_purchase_analysis(columns, replace_dict, build_network_graph, analyze_full_dataset)
    
    if analyze_meeting_data:
        perform_meeting_analysis(use_preprocess, columns)
    
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
    analyze_full_dataset = False
    analyze_meeting_data = True
    described_data, purchases_described = _main(columns, data_types, replace_dict,
                                                use_preprocess, deep_purchase_analysis, 
                                                data_describe_processing, compare_purchase_gail,
                                                build_network_graph, analyze_full_dataset, analyze_meeting_data)
        
    
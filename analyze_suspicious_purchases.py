# -*- coding: utf-8 -*-
"""
Created on Tue Aug 14 21:11:17 2018

@author: mitch
"""
import pandas as pd
import networkx as nx

import gather_purchase_metrics as gather_purchase_metrics
import date_time_conversion as dt_converter
import add_names_to_df as get_company_names


def analyze_confirmed_suspicious(columns, replace_dict, main_df, build_network_graph, output_dict):
    sus_purchase = pd.read_csv('Suspicious_purchases.csv', names=columns)
    sus_purchase = dt_converter.convert_time(sus_purchase)
    sus_purchase = get_company_names.add_names_to_data_frame(sus_purchase)
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
    layers, unique_mtg_attendees = gather_purchase_metrics.determine_layers_out(sus_df, sus_purchase_row, 
                                                          output_dict, suspicious_conf)
    print('only_sus_df')
    filename = 'confirmed_suspicious/confirmed_suspicious.csv'  
    analysis_type = 'confirmed_suspicious'
    network_df = gather_purchase_metrics.purchase_analysis(sus_purchase_row, sus_df, layers,
                                                           output_dict, suspicious_conf, 
                                                           filename, replace_dict,
                                                           unique_mtg_attendees,
                                                           analysis_type)
    
    if build_network_graph:
        perform_network_analysis(network_df)

    print('full_df')
    filename = 'confirmed_suspicious/confirmed_suspicious_full_set.csv'
    network_df = gather_purchase_metrics.purchase_analysis(sus_purchase_row, main_df, layers, 
                                                           output_dict, suspicious_conf, 
                                                           filename, replace_dict,
                                                           unique_mtg_attendees,
                                                           analysis_type)
    
    if build_network_graph:
        perform_network_analysis(network_df)
        
    #print(gather_purchase_metrics.determine_metrics_for_purchase(sus_analysis_full_df))
    
    #print('only_sus_df')
    #return gather_purchase_metrics.determine_metrics_for_purchase(sus_analysis_df), layers
    return layers, unique_mtg_attendees

def analyze_suspected_suspicious(main_df, replace_dict, layers, build_network_graph, output_dict, unique_mtg_attendees):
    other_suspicious = pd.read_csv('Other_suspicious_purchases.csv')
    other_suspicious.rename(columns={'Dest':'Destination','Time':'TimeStamp'}, inplace=True)
    other_suspicious = dt_converter.convert_time(other_suspicious)
    other_suspicious = get_company_names.add_names_to_data_frame(other_suspicious)
    suspicious_conf = 1
    analysis_type = 'suspected_suspicious'
    for index, rows in other_suspicious.iterrows():
        filename = ('suspected_suspicious/suspected_suspicous_{}_{}_{}.csv'.format(rows['Source'], 
                                                                                   rows['Destination'],
                                                                                   rows['TimeStamp']))
        network_df = gather_purchase_metrics.purchase_analysis(rows, main_df, layers, 
                                                               output_dict, suspicious_conf, 
                                                               filename, replace_dict,
                                                               unique_mtg_attendees,
                                                               analysis_type)
        if build_network_graph:
            perform_network_analysis(network_df)
        #print(gather_purchase_metrics.determine_metrics_for_purchase(analysis_df))

def analyze_all_purchases(main_df, replace_dict, purchase_df, layers, build_network_graph, output_dict, unique_mtg_attendees):
    purchase_df.reset_index(inplace=True)
    purchase_df = purchase_df.loc[6500:6600]
    purchase_df = get_company_names.add_names_to_data_frame(purchase_df)
    print('In all purchase region, not logging all runs on')
    suspicious_conf = -1
    analysis_type = 'normal_purchase'
    for index, rows in purchase_df.iterrows():
        if (index % 1000) == 1:
            print('index {}'.format(index))
        filename = ('normal_purchase/normal_purchase_{}_{}_{}.csv'.format(rows['Source'], 
                                                                          rows['Destination'],
                                                                          rows['TimeStamp']))
            
        network_df = gather_purchase_metrics.purchase_analysis(rows, main_df, layers, 
                                                               output_dict, suspicious_conf, 
                                                               filename, replace_dict,
                                                               unique_mtg_attendees,
                                                               analysis_type)
        if build_network_graph:
            perform_network_analysis(network_df)

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
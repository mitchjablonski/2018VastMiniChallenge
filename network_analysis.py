# -*- coding: utf-8 -*-
"""
Created on Thu Aug 16 23:13:56 2018

@author: mitch
"""

import pandas as pd
import networkx as nx
import glob as glob
from collections import defaultdict

def analyze_graphs(my_df, report_dict, layer):
    #G=nx.Graph()
    G = nx.from_pandas_edgelist(my_data, 'Source_Names', 'Destination_Names', edge_attr=None, create_using=None)
    
    report_dict['knearestscore'].append(nx.k_nearest_neighbors(G))
    report_dict['knearestscore'].append(nx.average_degree_connectivity(G))
    purchase = my_data.loc[my_data['Etype'] == 5]
    report_dict['Source_Names'].append(purchase['Source_Names'].iloc[0])
    report_dict['Destination_Names'].append(purchase['Destination_Names'].iloc[0])
    report_dict['full_date'].append(purchase['full_date'].iloc[0])
    report_dict['layer'].append(layer)
    
for layer in [1,2,3]:
    files = glob.glob('purchase_communication_results/*layer_{}.csv'.format(layer))
    for file in files:
        my_data = pd.read_csv(str(file))
        report_dict = defaultdict(list)
        analyze_graphs(my_data, report_dict, layer)
        result_df = pd.DataFrame.from_dict(report_dict, orient='index').transpose()
        result_df.to_csv('purchase_communication_results/network_analysis_data_df.csv')
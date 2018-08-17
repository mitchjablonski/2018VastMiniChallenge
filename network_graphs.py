# -*- coding: utf-8 -*-
"""
Created on Thu Aug 16 22:47:50 2018

@author: mitch
"""


import numpy as np
import pandas as pd
import holoviews as hv
import networkx as nx
import glob

import holoviews.plotting.bokeh

def render_graphs(input_df, layer):
    padding = dict(x=(-1.2, 1.2), y=(-1.2, 1.2))
    purchase_row = my_data.loc[my_data['Etype'] == 5].iloc[0]
    title_graph = '{}  Layer {}'.format(purchase_row['Source_Names'], layer)
    
    simple_graph = hv.Graph(((input_df['Source_Names'], input_df['Destination_Names']),), label=title_graph).redim.range(**padding)
    simple_graph()
    simple_graph.options(inspection_policy='edges', show_title=True)
    #hv.Layout([simple_graph.relabel(label=title)])
    hv.renderer('bokeh').server_doc(simple_graph)
    
hv.extension('bokeh')
for layer in [1,2,3]: 
    files = glob.glob('purchase_communication_results/*layer_{}.csv'.format(layer))
    for file in files:
        my_data = pd.read_csv(str(file))
        render_graphs(my_data, layer)
    

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

def render_graphs(input_df):
    N = 8
    padding = dict(x=(-1.2, 1.2), y=(-1.2, 1.2))
    
    simple_graph = hv.Graph(((input_df['Source_Names'], input_df['Destination_Names']),)).redim.range(**padding)
    simple_graph
    simple_graph.options(inspection_policy='edges')
    hv.renderer('bokeh').server_doc(simple_graph)
    
files = glob.glob('purchase_communication_results/*layer_2.csv')
for file in files:
    my_data = pd.read_csv(str(file))
    render_graphs(my_data)
    
hv.extension('bokeh')

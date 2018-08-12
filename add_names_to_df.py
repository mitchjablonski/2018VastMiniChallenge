# -*- coding: utf-8 -*-
"""
Created on Sun Aug 12 13:08:28 2018

@author: mitch
"""
import pandas as pd

def add_names_to_data_frame(input_df):
    return_df = input_df.copy()
    company_index     = pd.read_csv('CompanyIndex.csv')
    company_index['names'] = company_index['first'] + ' ' + company_index['last']
    return_df = return_df.merge(company_index, how='inner', left_on='Source', right_on ='ID')
    return_df = return_df.rename(columns={'names':'Source_Names'})
    drop_cols = ['first', 'last', 'ID']
    return_df.drop(drop_cols, axis=1, inplace=True)
    return_df = return_df.merge(company_index, how='inner', left_on='Destination', right_on ='ID')
    return_df = return_df.rename(columns={'names':'Destination_Names'})
    return_df.drop(drop_cols, axis=1, inplace=True)
    return return_df
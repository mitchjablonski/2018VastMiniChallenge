# -*- coding: utf-8 -*-
"""
Created on Wed Aug  8 21:49:01 2018

@author: mitch
"""

import pandas as pd
from datetime import datetime

def convert_time(input_df):
    return_df = input_df.copy()
    start_time = 1431316800
    return_df['TimeStamp'] += start_time
    return_df['full_date'] = pd.to_datetime(return_df['TimeStamp'], unit='s')
    return_df['month_day_yr'] = return_df['full_date'].dt.strftime('%m/%d/%Y')
    return_df['month_yr'] = return_df['full_date'].dt.strftime('%m/%Y')
    return_df['month'] = return_df['full_date'].dt.strftime('%m')
    return_df['yr'] = return_df['full_date'].dt.strftime('%Y')
    return return_df
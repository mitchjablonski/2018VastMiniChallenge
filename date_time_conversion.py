# -*- coding: utf-8 -*-
"""
Created on Wed Aug  8 21:49:01 2018

@author: mitch
"""

import pandas as pd
from datetime import datetime

def convert_time(input_df):
    start_time = 1431316800
    input_df['TimeStamp'] += start_time
    input_df['full_date'] = pd.to_datetime(input_df['TimeStamp'], unit='s')
    input_df['month_day_yr'] = input_df['full_date'].dt.strftime('%m/%d/%Y')
    input_df['month_yr'] = input_df['full_date'].dt.strftime('%m/%Y')
    input_df['month'] = input_df['full_date'].dt.strftime('%m')
    input_df['yr'] = input_df['full_date'].dt.strftime('%Y')
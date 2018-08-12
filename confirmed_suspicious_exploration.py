# -*- coding: utf-8 -*-
"""
Created on Tue Aug  7 20:51:53 2018

@author: mitch
"""
import pandas as pd
import date_time_conversion as dt_converter

columns = ['Source', 'Etype', 'Destination', 'TimeStamp']
#data_types = ['calls', 'emails', 'purchases', 'meetings']
email_data = pd.read_csv(r'C:\Users\mitch\Desktop\Masters\VisualAnalytics\2018MiniChallenge\Suspicious_emails.csv', header=None, names = columns)
call_data  =  pd.read_csv(r'C:\Users\mitch\Desktop\Masters\VisualAnalytics\2018MiniChallenge\Suspicious_calls.csv', header=None, names = columns)
meeting_data =  pd.read_csv(r'C:\Users\mitch\Desktop\Masters\VisualAnalytics\2018MiniChallenge\Suspicious_meetings.csv', header=None, names = columns)
purchase_data = pd.read_csv(r'C:\Users\mitch\Desktop\Masters\VisualAnalytics\2018MiniChallenge\Suspicious_purchases.csv', header=None, names = columns)
company_index = pd.read_csv(r'C:\Users\mitch\Desktop\Masters\VisualAnalytics\2018MiniChallenge\CompanyIndex.csv')

company_index['names'] = company_index['first'] + ' ' + company_index['last']

main_df = pd.concat([email_data, call_data, meeting_data, purchase_data], ignore_index=True)

main_df = main_df.merge(company_index, how='inner', left_on='Source', right_on ='ID')
main_df = main_df[['Source', 'Etype', 'Destination', 'TimeStamp','names']].rename(columns={'names':'Source_Names'})

main_df = main_df.merge(company_index, how='inner', left_on='Destination', right_on ='ID')
main_df = main_df[['Source', 'Etype', 'Destination', 'TimeStamp','Source_Names', 'names']].rename(columns={'names':'Destination_Names'})
main_df.sort_values(by='TimeStamp', inplace=True)
main_df.reset_index(inplace=True, drop=True)
dt_converter.convert_time(main_df)

replace_dict = {0:'calls',1:'emails',2:'purchases',3:'meetings'}
main_df.replace(replace_dict, inplace=True)
main_df.to_csv('combinded_suspicious.csv',index=False)


only_purchases = pd.read_csv(r'C:\Users\mitch\Desktop\Masters\VisualAnalytics\2018MiniChallenge\purchases.csv', header=None, names = columns)
only_purchases['Source'].nunique()
only_purchases['Destination'].nunique()
##Only 459 people are making the purchases
only_purchases['Destination'].value_counts()

purchase_detail = pd.DataFrame(only_purchases['Destination'].unique(), columns=['Destination'])

purchase_detail = purchase_detail.merge(pd.DataFrame(only_purchases['Destination'].value_counts()), how='left', left_on='Destination', right_index=True)
purchase_detail.rename(columns={'Destination_x':'Destination','Destination_y':'Purchase_Count'}, inplace=True)
purchase_detail = purchase_detail.merge(company_index, how='inner', left_on='Destination', right_on ='ID')
purchase_detail = purchase_detail[['Destination','Purchase_Count', 'names']].rename(columns={'names':'Destination_Names'})
purchase_detail.sort_values(by='Purchase_Count', inplace=True)
purchase_detail.to_csv('purchase_detail.csv')
# -*- coding: utf-8 -*-
"""
Created on Wed Sep 20 15:30:30 2023

@author: carlos
"""

import pandas as pd

data = pd.read_excel('FMR_2022_clean_all_final.xlsx')
"""
#This checks how many are within country or to different countries

data['Migration_Type'] = ['intra-country %' if depart == dest else 'inter-country %' 
                          for depart, dest in zip(data['Departed_country'], data['Destination_country'])]
migration_counts = data['Migration_Type'].value_counts(normalize=True) * 100
print(migration_counts)

#This produces routes between countries by frequency
country_to_country_migrations = data.groupby(['Departed_country', 'Destination_country']).size().reset_index(name='Migration_Counts')
common_routes_country = country_to_country_migrations.sort_values(by="Migration_Counts", ascending=False).reset_index(drop=True)

#Routes between admin1s by frequency
admin1_migrations = data.groupby(['Departed_admin1', 'Destination_admin1']).size().reset_index(name='Migration_Counts_Admin1')
common_routes_admin1 = admin1_migrations.sort_values(by="Migration_Counts_Admin1", ascending=False).reset_index(drop=True)

#Routes between admin2s by frequency
admin2_migrations = data.groupby(['Departed_admin2', 'Destination_admin2']).size().reset_index(name='Migration_Counts_Admin2')
common_routes_admin2 = admin2_migrations.sort_values(by="Migration_Counts_Admin2", ascending=False).reset_index(drop=True)

#This ranks weeks by number of migrations to check for migration shocks as opposed to regular flows
data['Info/_8_DATE'] = pd.to_datetime(data['Info/_8_DATE'])
data.set_index('Info/_8_DATE', inplace=True)
outflows = data.groupby('Departed_admin1').resample('W').size().reset_index()
outflows.columns = ['Departed_admin1', 'Week', 'Outflows']
outflows_sorted = outflows.sort_values('Outflows', ascending=False).reset_index(drop=True)
"""
# This does the same but by month
data['Info/_8_DATE'] = pd.to_datetime(data['Info/_8_DATE'])
data.set_index('Info/_8_DATE', inplace=True)
outflows_month = data.groupby('Departed_admin1').resample('M').size().reset_index()
outflows_month.columns = ['Departed_admin1', 'Month', 'Outflows']
outflows_month_sorted = outflows_month.sort_values('Outflows', ascending=False).reset_index(drop=True)


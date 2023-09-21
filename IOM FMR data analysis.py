# -*- coding: utf-8 -*-
"""
Created on Wed Sep 20 15:30:30 2023

@author: carlos
"""

import pandas as pd

data = pd.read_excel('FMR_2022_clean_all_final.xlsx')
data["_16_DISAG_BY_SEX_and_AGE/_16_3_total_pers"] = pd.to_numeric(data["_16_DISAG_BY_SEX_and_AGE/_16_3_total_pers"], errors='coerce').fillna(0)

#This checks how many are within country or to different countries

data['Migration_Type'] = ['intra-country %' if depart == dest else 'inter-country %' 
                          for depart, dest in zip(data['Departed_country'], data['Destination_country'])]
intra_inter_country_percentages = data['Migration_Type'].value_counts(normalize=True) * 100

#This produces routes between countries by frequency
country_to_country_migrations = data.groupby(['Departed_country', 'Destination_country'])["_16_DISAG_BY_SEX_and_AGE/_16_3_total_pers"].sum().reset_index()
common_routes_country = country_to_country_migrations.sort_values(by="_16_DISAG_BY_SEX_and_AGE/_16_3_total_pers", ascending=False).reset_index(drop=True)

#Routes between admin1s
admin1_migrations = data.groupby(['Departed_admin1', 'Destination_admin1'])["_16_DISAG_BY_SEX_and_AGE/_16_3_total_pers"].sum().reset_index(name='Migration_Counts_Admin1')
common_routes_admin1 = admin1_migrations.sort_values(by="Migration_Counts_Admin1", ascending=False).reset_index(drop=True)

# Routes between admin2s
admin2_migrations = data.groupby(['Departed_admin2', 'Destination_admin2'])["_16_DISAG_BY_SEX_and_AGE/_16_3_total_pers"].sum().reset_index(name='Migration_Counts_Admin2')
common_routes_admin2 = admin2_migrations.sort_values(by="Migration_Counts_Admin2", ascending=False).reset_index(drop=True)

#put date format into correct pd format
data['Info/_8_DATE'] = pd.to_datetime(data['Info/_8_DATE'])
data.set_index('Info/_8_DATE', inplace=True)

#This ranks weeks by number of migrations from admin1s to check for migration shocks as opposed to regular flows
outflows_week = data.groupby(['Departed_country', 'Departed_admin1']).resample('W')["_16_DISAG_BY_SEX_and_AGE/_16_3_total_pers"].sum().reset_index()
outflows_week.columns = ['Departed_country', 'Departed_admin1', 'Week', 'Outflows']
outflows_week_sorted = outflows_week.sort_values('Outflows', ascending=False).reset_index(drop=True)

# This does the same but by month
outflows_month = data.groupby(['Departed_country', 'Departed_admin1']).resample('M')["_16_DISAG_BY_SEX_and_AGE/_16_3_total_pers"].sum().reset_index()
outflows_month.columns = ['Departed_country', 'Departed_admin1', 'Month', 'Outflows']
outflows_month_sorted = outflows_month.sort_values('Outflows', ascending=False).reset_index(drop=True)


#vulnerability analysis
vulnerability_cols = [
    '_17_VULNERABILITIES/_17_1_PREGNANT_AND_LACTATING',
    '_17_VULNERABILITIES/_17_2_OF_CHILDREN_UNDER_5',
    '_17_VULNERABILITIES/_17_3_UNACCOMP_CHILD',
    '_17_VULNERABILITIES/_17_4_PHYSICAL_DISABILITY',
    '_17_VULNERABILITIES/_17_5_ELDERLY_60'
]
data[vulnerability_cols] = data[vulnerability_cols].apply(pd.to_numeric, errors='coerce')
data[vulnerability_cols] = data[vulnerability_cols].fillna(0)
filtered_data = data[data[vulnerability_cols].ne(0).any(axis=1)]
#vulnerabilities by admin1 departure
vulnerability_by_admin1_departure = filtered_data.groupby('Departed_admin1')[vulnerability_cols].sum().reset_index()
vulnerability_by_admin1_departure['Total_Vulnerabilities'] = vulnerability_by_admin1_departure[vulnerability_cols].sum(axis=1)

#vulnerabilities by admin2 departure
vulnerability_by_admin2_departure = filtered_data.groupby('Departed_admin2')[vulnerability_cols].sum().reset_index()
vulnerability_by_admin2_departure['Total_Vulnerabilities'] = vulnerability_by_admin2_departure[vulnerability_cols].sum(axis=1)

#vulnerabilities by admin1 destination
vulnerability_by_admin1_destination = filtered_data.groupby('Destination_admin1')[vulnerability_cols].sum().reset_index()
vulnerability_by_admin1_destination['Total_Vulnerabilities'] = vulnerability_by_admin1_destination[vulnerability_cols].sum(axis=1)

#vulnerabilities by admin2 destination
vulnerability_by_admin2_destination = filtered_data.groupby('Destination_admin2')[vulnerability_cols].sum().reset_index()
vulnerability_by_admin2_destination['Total_Vulnerabilities'] = vulnerability_by_admin2_destination[vulnerability_cols].sum(axis=1)

#Total vulnerability statistics
total_vulnerability_stats = data[vulnerability_cols].sum()

#Sex and age disaggregation analysis
disaggregation_cols = [
    '_16_DISAG_BY_SEX_and_AGE/_16_1_FEMALE/_16_1a_Children_below_18_',
    '_16_DISAG_BY_SEX_and_AGE/_16_1_FEMALE/_16_1b_Adults_18_and_above',
    '_16_DISAG_BY_SEX_and_AGE/_16_2_MALE/_16_2a_Children_below_18_',
    '_16_DISAG_BY_SEX_and_AGE/_16_2_MALE/_16_2b_Adults_18_and_above_',
    '_16_DISAG_BY_SEX_and_AGE/_16_3_total_pers',
    '_16_DISAG_BY_SEX_and_AGE/total_number_persons',
    '_16_DISAG_BY_SEX_and_AGE/note_total_persons'
]
data[disaggregation_cols] = data[disaggregation_cols].apply(pd.to_numeric, errors='coerce')
data[disaggregation_cols] = data[disaggregation_cols].fillna(0)

sex_age_departure_admin1 = data.groupby('Departed_admin1')[disaggregation_cols].sum().reset_index()
sex_age_departure_admin2 = data.groupby('Departed_admin2')[disaggregation_cols].sum().reset_index()
sex_age_destination_admin1 = data.groupby('Destination_admin1')[disaggregation_cols].sum().reset_index()
sex_age_destination_admin2 = data.groupby('Destination_admin2')[disaggregation_cols].sum().reset_index()

#Sex proportions departed admin2
gender_sums_dept_admin2 = data.groupby('Departed_admin2').agg({
    '_16_DISAG_BY_SEX_and_AGE/_16_1_FEMALE/_16_1a_Children_below_18_': 'sum',
    '_16_DISAG_BY_SEX_and_AGE/_16_1_FEMALE/_16_1b_Adults_18_and_above': 'sum',
    '_16_DISAG_BY_SEX_and_AGE/_16_2_MALE/_16_2a_Children_below_18_': 'sum',
    '_16_DISAG_BY_SEX_and_AGE/_16_2_MALE/_16_2b_Adults_18_and_above_': 'sum'
}).reset_index()
# Calculate total females and males for each admin2
gender_sums_dept_admin2['Total_Females'] = gender_sums_dept_admin2['_16_DISAG_BY_SEX_and_AGE/_16_1_FEMALE/_16_1a_Children_below_18_'] + gender_sums_dept_admin2['_16_DISAG_BY_SEX_and_AGE/_16_1_FEMALE/_16_1b_Adults_18_and_above']
gender_sums_dept_admin2['Total_Males'] = gender_sums_dept_admin2['_16_DISAG_BY_SEX_and_AGE/_16_2_MALE/_16_2a_Children_below_18_'] + gender_sums_dept_admin2['_16_DISAG_BY_SEX_and_AGE/_16_2_MALE/_16_2b_Adults_18_and_above_']
# Calculate the proportions
gender_sums_dept_admin2['Proportion_Females'] = gender_sums_dept_admin2['Total_Females'] / (gender_sums_dept_admin2['Total_Females'] + gender_sums_dept_admin2['Total_Males'])
gender_sums_dept_admin2['Proportion_Males'] = gender_sums_dept_admin2['Total_Males'] / (gender_sums_dept_admin2['Total_Females'] + gender_sums_dept_admin2['Total_Males'])
# Keep only the relevant columns
gender_proportions_departed_admin2 = gender_sums_dept_admin2[['Departed_admin2', 'Total_Females', 'Total_Males', 'Proportion_Females', 'Proportion_Males']]

#Sex proportions destination admin2
gender_sums_dest_admin2 = data.groupby('Destination_admin2').agg({
    '_16_DISAG_BY_SEX_and_AGE/_16_1_FEMALE/_16_1a_Children_below_18_': 'sum',
    '_16_DISAG_BY_SEX_and_AGE/_16_1_FEMALE/_16_1b_Adults_18_and_above': 'sum',
    '_16_DISAG_BY_SEX_and_AGE/_16_2_MALE/_16_2a_Children_below_18_': 'sum',
    '_16_DISAG_BY_SEX_and_AGE/_16_2_MALE/_16_2b_Adults_18_and_above_': 'sum'
}).reset_index()
gender_sums_dest_admin2['Total_Females'] = gender_sums_dest_admin2['_16_DISAG_BY_SEX_and_AGE/_16_1_FEMALE/_16_1a_Children_below_18_'] + gender_sums_dest_admin2['_16_DISAG_BY_SEX_and_AGE/_16_1_FEMALE/_16_1b_Adults_18_and_above']
gender_sums_dest_admin2['Total_Males'] = gender_sums_dest_admin2['_16_DISAG_BY_SEX_and_AGE/_16_2_MALE/_16_2a_Children_below_18_'] + gender_sums_dest_admin2['_16_DISAG_BY_SEX_and_AGE/_16_2_MALE/_16_2b_Adults_18_and_above_']
gender_sums_dest_admin2['Proportion_Females'] = gender_sums_dest_admin2['Total_Females'] / (gender_sums_dest_admin2['Total_Females'] + gender_sums_dest_admin2['Total_Males'])
gender_sums_dest_admin2['Proportion_Males'] = gender_sums_dest_admin2['Total_Males'] / (gender_sums_dest_admin2['Total_Females'] + gender_sums_dest_admin2['Total_Males'])
gender_proportions_destined_admin2 = gender_sums_dest_admin2[['Destination_admin2', 'Total_Females', 'Total_Males', 'Proportion_Females', 'Proportion_Males']]

#Sex proportions departed admin1
gender_sums_dept_admin1 = data.groupby('Departed_admin1').agg({
    '_16_DISAG_BY_SEX_and_AGE/_16_1_FEMALE/_16_1a_Children_below_18_': 'sum',
    '_16_DISAG_BY_SEX_and_AGE/_16_1_FEMALE/_16_1b_Adults_18_and_above': 'sum',
    '_16_DISAG_BY_SEX_and_AGE/_16_2_MALE/_16_2a_Children_below_18_': 'sum',
    '_16_DISAG_BY_SEX_and_AGE/_16_2_MALE/_16_2b_Adults_18_and_above_': 'sum'
}).reset_index()
gender_sums_dept_admin1['Total_Females'] = gender_sums_dept_admin1['_16_DISAG_BY_SEX_and_AGE/_16_1_FEMALE/_16_1a_Children_below_18_'] + gender_sums_dept_admin1['_16_DISAG_BY_SEX_and_AGE/_16_1_FEMALE/_16_1b_Adults_18_and_above']
gender_sums_dept_admin1['Total_Males'] = gender_sums_dept_admin1['_16_DISAG_BY_SEX_and_AGE/_16_2_MALE/_16_2a_Children_below_18_'] + gender_sums_dept_admin1['_16_DISAG_BY_SEX_and_AGE/_16_2_MALE/_16_2b_Adults_18_and_above_']
gender_sums_dept_admin1['Proportion_Females'] = gender_sums_dept_admin1['Total_Females'] / (gender_sums_dept_admin1['Total_Females'] + gender_sums_dept_admin1['Total_Males'])
gender_sums_dept_admin1['Proportion_Males'] = gender_sums_dept_admin1['Total_Males'] / (gender_sums_dept_admin1['Total_Females'] + gender_sums_dept_admin1['Total_Males'])
gender_proportions_departed_admin1 = gender_sums_dept_admin1[['Departed_admin1', 'Total_Females', 'Total_Males', 'Proportion_Females', 'Proportion_Males']]

#Sex proportions destination admin1
gender_sums_dest_admin1 = data.groupby('Destination_admin1').agg({
    '_16_DISAG_BY_SEX_and_AGE/_16_1_FEMALE/_16_1a_Children_below_18_': 'sum',
    '_16_DISAG_BY_SEX_and_AGE/_16_1_FEMALE/_16_1b_Adults_18_and_above': 'sum',
    '_16_DISAG_BY_SEX_and_AGE/_16_2_MALE/_16_2a_Children_below_18_': 'sum',
    '_16_DISAG_BY_SEX_and_AGE/_16_2_MALE/_16_2b_Adults_18_and_above_': 'sum'
}).reset_index()
gender_sums_dest_admin1['Total_Females'] = gender_sums_dest_admin1['_16_DISAG_BY_SEX_and_AGE/_16_1_FEMALE/_16_1a_Children_below_18_'] + gender_sums_dest_admin1['_16_DISAG_BY_SEX_and_AGE/_16_1_FEMALE/_16_1b_Adults_18_and_above']
gender_sums_dest_admin1['Total_Males'] = gender_sums_dest_admin1['_16_DISAG_BY_SEX_and_AGE/_16_2_MALE/_16_2a_Children_below_18_'] + gender_sums_dest_admin1['_16_DISAG_BY_SEX_and_AGE/_16_2_MALE/_16_2b_Adults_18_and_above_']
gender_sums_dest_admin1['Proportion_Females'] = gender_sums_dest_admin1['Total_Females'] / (gender_sums_dest_admin1['Total_Females'] + gender_sums_dest_admin1['Total_Males'])
gender_sums_dest_admin1['Proportion_Males'] = gender_sums_dest_admin1['Total_Males'] / (gender_sums_dest_admin1['Total_Females'] + gender_sums_dest_admin1['Total_Males'])
gender_proportions_destined_admin1 = gender_sums_dest_admin1[['Destination_admin1', 'Total_Females', 'Total_Males', 'Proportion_Females', 'Proportion_Males']]

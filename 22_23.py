# -*- coding: utf-8 -*-
"""
Created on Thu Mar 18 08:11:45 2021

@author: dpedroja
"""

import pandas as pd
import numpy as np
import import_functions as imp

# this routine supports flags 22 and 23

# first get length of diversion season in months (A)

# read in data
flat_file_season = imp.import_flat_file_season()
flag_22_data = flat_file_season
flat_file_data = imp.import_flat_file()

# replace NaN with 0
flag_22_data = flag_22_data.fillna(0)
# define diversion seasons
season_start = ["DIRECT_SEASON_START_MONTH_1", "DIRECT_SEASON_START_MONTH_2", "DIRECT_SEASON_START_MONTH_3"]
season_end = ["DIRECT_DIV_SEASON_END_MONTH_1", "DIRECT_DIV_SEASON_END_MONTH_2", "DIRECT_DIV_SEASON_END_MONTH_3"]
# define fields with lists as values
div_fields = ["DIVERSION_MONTHS", "DIVERSION_MONTHS_2", "DIVERSION_MONTHS_3"]
for field in div_fields:
    flag_22_data[field] = [[]] * len(flag_22_data) 

# populate list of lists of months
# TAKES A FEW MINUTES
for k, field in enumerate(season_start):
    for i in range(len(flag_22_data)):
        months = range(int(flag_22_data[season_start[k]][i]), 
        int(flag_22_data[season_end[k]][i]+1))
        flag_22_data[div_fields[k]][i] = [month for month in months] 
    for i in range(len(flag_22_data)):
        print(k,i)
        if (flag_22_data[season_start[k]][i] > flag_22_data[season_end[k]][i]):
            months = range(int(flag_22_data[season_start[k]][i]), 13) 
            months2 = range(1, int(flag_22_data[season_end[k]][i]))
            flag_22_data[div_fields[k]][i] = [month for month in months]
            months2 = [month for month in months2]
            for element in months2:
                flag_22_data[div_fields[k]][i].append(element)
# define new field DIV_SEASON_LENGTH
flag_22_data["DIV_SEASON_LENGTH"]= "NA"            
# length of diversion season in months
for i, app in enumerate(flag_22_data["POD_ID"]):
    a = set(flag_22_data["DIVERSION_MONTHS"][i])
    a.discard(0)
    b = set(flag_22_data["DIVERSION_MONTHS_2"][i])
    b.discard(0)
    c = set(flag_22_data["DIVERSION_MONTHS_3"][i])
    c.discard(0)
    d = (a.union(b)).union(c)
    flag_22_data["DIV_SEASON_LENGTH"][i] = len(d)

# get face value amount from flat file & merge
flag_22_data = flag_22_data.merge(flat_file_data, how = "left", on = "WR_WATER_RIGHT_ID")

# calculate monthly amount as FACE_VALUE_AMOUNT divided by DIV_SEASON_LENGTH calculated above
flag_22_data["FV_PER_MONTH"] = [np.divide(flag_22_data["FACE_VALUE_AMOUNT"][i], flag_22_data["DIV_SEASON_LENGTH"][i], 
              out=np.zeros_like(flag_22_data["FACE_VALUE_AMOUNT"][i]), where=flag_22_data["DIV_SEASON_LENGTH"][i]!=0) 
     for i, right in enumerate(flag_22_data["FACE_VALUE_AMOUNT"])]

# create month field names
months = ["DIRECT_DIV_JANUARY", "DIRECT_DIV_FEBRUARY", "DIRECT_DIV_MARCH", "DIRECT_DIV_APRIL", 
          "DIRECT_DIV_MAY", "DIRECT_DIV_JUNE", "DIRECT_DIV_JULY", "DIRECT_DIV_AUGUST", 
          "DIRECT_DIV_SEPTEMBER", "DIRECT_DIV_OCTOBER", "DIRECT_DIV_NOVEMBER", "DIRECT_DIV_DECEMBER"]
for month in months:
    flag_22_data[month] = "NA"
# check each month against list
# COULD TAKE A WHILE (5 MINUTES OR MORE)
for j, month in enumerate(months):
    for i in range(len(flag_22_data)):
        print(j,i)
        flag_22_data[month][i] = np.where(
            ((j+1) in flag_22_data["DIVERSION_MONTHS"][i])  |
            ((j+1) in flag_22_data["DIVERSION_MONTHS_2"][i])  |
            ((j+1) in flag_22_data["DIVERSION_MONTHS_3"][i]),
            "1", "0")

# FLAG 22 loop

# define field names
monthly_dist_fv = ["JANUARY_DISTRIBUTION_FV", "FEBRUARY_DISTRIBUTION_FV", "MARCH_DISTRIBUTION_FV", "APRIL_DISTRIBUTION_FV", 
          "MAY_DISTRIBUTION_FV", "JUNE_DISTRIBUTION_FV", "JULY_DISTRIBUTION_FV", "AUGUST_DISTRIBUTION_FV", 
          "SEPTEMBER_DISTRIBUTION_FV", "OCTOBER_DISTRIBUTION_FV", "NOVEMBER_DISTRIBUTION_FV", "DECEMBER_DISTRIBUTION_FV"]
for field_name in monthly_dist_fv:
    flag_22_data[field_name] = "NA"
# define (January) distribution as DIRECT_DIV_JAN * FV_PER_MONTH
for k, month in enumerate(monthly_dist_fv):
    flag_22_data[monthly_dist_fv[k]] = (np.array(flag_22_data["FV_PER_MONTH"], dtype = float) * np.array(flag_22_data[months[k]], dtype = float)).round(3)

flag_22_data.to_csv("output\\f22_apportion_fv.csv")
# Flag 22 end

# Flag 23: Flag water rights with reported diversions outside of diversion season
# select a subset of fields
div_season_data = pd.DataFrame(flag_22_data[["WR_WATER_RIGHT_ID", "APPLICATION_NUMBER", "USE_CODE",
        "DIRECT_DIV_JANUARY", "DIRECT_DIV_FEBRUARY", "DIRECT_DIV_MARCH", "DIRECT_DIV_APRIL", 
        "DIRECT_DIV_MAY", "DIRECT_DIV_JUNE", "DIRECT_DIV_JULY", "DIRECT_DIV_AUGUST",
        "DIRECT_DIV_SEPTEMBER", "DIRECT_DIV_OCTOBER", "DIRECT_DIV_NOVEMBER", "DIRECT_DIV_DECEMBER"]])
# change back to Y / N data
div_season_data = div_season_data.replace("0", "N")
div_season_data = div_season_data.replace("1", "Y")
# import rms monthly data
rms_monthly = imp.import_rms_monthly(100000000000000)
rms_monthly.reset_index(inplace = True)
# merge data
flag_23_data = rms_monthly.merge(div_season_data, how = "inner", left_on = "APPL_ID", right_on = "APPLICATION_NUMBER")
# this merge multiplies a lot of records because of multiple use types per application number. 

# rms monthly reported diversion fields
div_reported = ["JANUARY_DIV", "FEBRUARY_DIV", "MARCH_DIV", "APRIL_DIV", 
              "MAY_DIV", "JUNE_DIV", "JULY_DIV", "AUGUST_DIV", 
              "SEPTEMBER_DIV", "OCTOBER_DIV", "NOVEMBER_DIV", "DECEMBER_DIV"]
# Field DIV_REPORTED_OUTSIDE_DIV_SEASON criteria
for n, month in enumerate(div_reported):
    flag_23_data["DIV_REPORTED_OUTSIDE_DIV_SEASON"] = np.where( 
        (flag_23_data[div_reported[n]] > 0)                          &  
        (flag_23_data[months[n]] == "N"),
        "Y", "N")

flag_23_data = flag_23_data[['WATER_RIGHT_ID', 'APPL_ID', 'YEAR', 
                 'DIRECT_DIV_JANUARY', 'DIRECT_DIV_FEBRUARY', 'DIRECT_DIV_MARCH',
                 'DIRECT_DIV_APRIL', 'DIRECT_DIV_MAY', 'DIRECT_DIV_JUNE',
                 'DIRECT_DIV_JULY', 'DIRECT_DIV_AUGUST', 'DIRECT_DIV_SEPTEMBER',
                 'DIRECT_DIV_OCTOBER', 'DIRECT_DIV_NOVEMBER', 'DIRECT_DIV_DECEMBER',
                 'DIV_REPORTED_OUTSIDE_DIV_SEASON']]

# number of records is greater than RMS because some applications have different diversion months for differenct use codes (apparently)
flag_23_data = flag_23_data.drop_duplicates()

flag_23_data.to_csv("output\\f23_div_out_of_season.csv")


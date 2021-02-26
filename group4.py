# -*- coding: utf-8 -*-
"""
Created on Thu Feb 25 10:00:14 2021

@author: dpedroja
"""

# THIS SCRIPT CAN TAKE SEVERAL MINUTES OR MORE

import pandas as pd
import numpy as np
import import_functions as imp

flat_file_data = imp.import_flat_file()
flat_file_season = imp.import_flat_file_season()

# Flag 7: Identify water rights with Non-Consumptive Uses to account for water use returning water diverted back to the system
# merge data
flag_7_data = flat_file_season.merge(flat_file_data, how = "left", on = "WR_WATER_RIGHT_ID")

# new field: USE_RETURN
# value criteria

flag_7_data["USE_RETURN"] = np.where( 
    (flag_7_data["USE_CODE"] == "Power")            |       
    (flag_7_data["USE_CODE"] == "Aquaculture")      &                 
    (
    (flag_7_data["USE_STORAGE_AMOUNT"].notna())     |
    (flag_7_data["USE_DIRECT_DIV_ANNUAL_AMOUNT"].notna())
    ),
    flag_7_data["USE_STORAGE_AMOUNT"] + flag_7_data["USE_DIRECT_DIV_ANNUAL_AMOUNT"], 0)


##############################################################
# Flag 17: Classify records into groups by “Beneficial Use Types” - identify dominant BU (by sorting or other logic) when multiple BU’s are listed

# merge data
flag_17_data = flat_file_season.merge(flat_file_data, how = "left", on = "WR_WATER_RIGHT_ID")
# Beneficial use types
vals = sorted(flag_17_data["USE_CODE"].unique())
# create field headings
fields = []
for i, item in enumerate(vals):
    fields.append(vals[i].replace(" ", "_").upper())
    
for i, use in enumerate(vals):
    flag_17_data[fields[i]] = np.where( 
    (flag_17_data["USE_CODE"] == vals[i]),               
    "Y", "N")

# Flag 23: Flag water rights with reported diversions outside of diversion season
# read in data
flag_23_data = flat_file_season
# replace NaN with 0
flag_23_data = flag_23_data.fillna(0)
# define diversion seasons
season_start = ["DIRECT_SEASON_START_MONTH_1", "DIRECT_SEASON_START_MONTH_2", "DIRECT_SEASON_START_MONTH_3"]
season_end = ["DIRECT_DIV_SEASON_END_MONTH_1", "DIRECT_DIV_SEASON_END_MONTH_2", "DIRECT_DIV_SEASON_END_MONTH_3"]
# define fields
div_fields = ["DIVERSION_MONTHS", "DIVERSION_MONTHS_2", "DIVERSION_MONTHS_3"]
for field in div_fields:
    flag_23_data[field] = [[]] *len(flag_23_data) 

# generate list of lists
# TAKES A FEW MINUTES
for k, field in enumerate(season_start):
    for i in range(len(flag_23_data)):
        months = range(int(flag_23_data[season_start[k]][i]), 
        int(flag_23_data[season_end[k]][i]+1))
        flag_23_data[div_fields[k]][i] = [month for month in months] 
    for i in range(len(flag_23_data)):
        if (flag_23_data[season_start[k]][i] > flag_23_data[season_end[k]][i]):
            months = range(int(flag_23_data[season_start[k]][i]), 13) 
            months2 = range(1, int(flag_23_data[season_end[k]][i]))
            flag_23_data[div_fields[k]][i] = [month for month in months]
            months2 = [month for month in months2]
            for element in months2:
                flag_23_data[div_fields[k]][i].append(element)
# create month field names
months = ["DIRECT_DIV_JANUARY", "DIRECT_DIV_FEBRUARY", "DIRECT_DIV_MARCH", "DIRECT_DIV_APRIL", 
          "DIRECT_DIV_MAY", "DIRECT_DIV_JUNE", "DIRECT_DIV_JULY", "DIRECT_DIV_AUGUST", 
          "DIRECT_DIV_SEPTEMBER", "DIRECT_DIV_OCTOBER", "DIRECT_DIV_NOVEMBER", "DIRECT_DIV_DECEMBER"]
for month in months:
    flag_23_data[month] = "NA"
# check each month against list
# COULD TAKE A WHILE (5 MINUTES OR MORE)
for j, month in enumerate(months):
    for i in range(len(flag_23_data)):
        flag_23_data[month][i] = np.where(
            ((j+1) in flag_23_data["DIVERSION_MONTHS"][i])  |
            ((j+1) in flag_23_data["DIVERSION_MONTHS_2"][i])  |
            ((j+1) in flag_23_data["DIVERSION_MONTHS_3"][i]),
            "Y", "N")
# temporary
flag_23_data.to_csv("eWRIMS_data/flag_23_data.csv")
flag_23_data = pd.read_csv("eWRIMS_data/flag_23_data.csv")
div_season_data = pd.DataFrame(flag_23_data[["WR_WATER_RIGHT_ID", "APPLICATION_NUMBER", "USE_CODE",
        "DIRECT_DIV_JANUARY", "DIRECT_DIV_FEBRUARY", "DIRECT_DIV_MARCH", "DIRECT_DIV_APRIL", 
        "DIRECT_DIV_MAY", "DIRECT_DIV_JUNE", "DIRECT_DIV_JULY", "DIRECT_DIV_AUGUST",
        "DIRECT_DIV_SEPTEMBER", "DIRECT_DIV_OCTOBER", "DIRECT_DIV_NOVEMBER", "DIRECT_DIV_DECEMBER"]])
del(flag_23_data)

# import rms monthly data
rms_monthly = imp.import_rms_monthly(100000000000000)
rms_monthly.reset_index(inplace = True)
# merge data
flag_23_merge = rms_monthly.merge(div_season_data, how = "inner", left_on = "APPL_ID", right_on = "APPLICATION_NUMBER")
# this merge multiplies a lot of records because of multiple use types per application number. 
# rms monthly reported diversion fields
div_reported = ["JANUARY_DIV", "FEBRUARY_DIV", "MARCH_DIV", "APRIL_DIV", 
              "MAY_DIV", "JUNE_DIV", "JULY_DIV", "AUGUST_DIV", 
              "SEPTEMBER_DIV", "OCTOBER_DIV", "NOVEMBER_DIV", "DECEMBER_DIV"]
# Field DIV_REPORTED_OUTSIDE_DIV_SEASON criteria
for n, month in enumerate(div_reported):
    flag_23_merge["DIV_REPORTED_OUTSIDE_DIV_SEASON"] = np.where( 
        (flag_23_merge[div_reported[n]] > 0)                          &  
        (flag_23_merge[months[n]] == "N"),
        "Y", "N")
# remove use code so the table can be condensed
flag_23_merge.drop(["USE_CODE"], axis = 1, inplace = True)
# number of records is greater than RMS because some applications have different diversion months for differenct use codes (apparentely)
flag_23_merge.drop_duplicates(inplace = True)


#     {'trial_num': [1, 2, 3, 1, 2, 3],
#      'subject': [1, 1, 1, 2, 2, 2],
#      'samples': [list(np.random.randn(3).round(2)) for i in range(6)]
#     }
# )


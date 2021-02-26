# -*- coding: utf-8 -*-
"""
Created on Thu Feb 25 10:00:14 2021

@author: dpedroja
"""


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

flag_23_data = flag_23_data.fillna(0)
# generate list of lists
flag_23_data["DIVERSION_MONTHS"] = [[]] * len(flag_23_data) 
for i in range(len(flag_23_data)):
    months = range(int(flag_23_data["DIRECT_SEASON_START_MONTH_1"][i]), 
    int(flag_23_data["DIRECT_DIV_SEASON_END_MONTH_1"][i]+1))
    flag_23_data["DIVERSION_MONTHS"][i] = [month for month in months] 
# create month field names
months = ["JANUARY", "FEBRUARY", "MARCH", "APRIL", "MAY", "JUNE", "JULY", "AUGUST", "SEPTEMBER", "OCTOBER", "NOVEMBER", "DECEMBER"]
for month in months:
    flag_23_data[month] = "NA"
    
# check each month against list
# THIS TAKES ABOUT 5 MINUTES!!!!!!!!!!!!
for j, month in enumerate(months):
    for i in range(len(flag_23_data)):
        flag_23_data[month][i] = np.where(
            (j+1) in flag_23_data["lst"][i],
            "Y", "N")
    









# df = pd.DataFrame(
#     {'trial_num': [1, 2, 3, 1, 2, 3],
#      'subject': [1, 1, 1, 2, 2, 2],
#      'samples': [list(np.random.randn(3).round(2)) for i in range(6)]
#     }
# )


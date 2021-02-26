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
# replace NaN with 0
flag_23_data = flag_23_data.fillna(0)

# generate list of lists
flag_23_data["DIVERSION_MONTHS"] = [[]] * len(flag_23_data) 
for i in range(len(flag_23_data)):
    months = range(int(flag_23_data["DIRECT_SEASON_START_MONTH_1"][i]), 
    int(flag_23_data["DIRECT_DIV_SEASON_END_MONTH_1"][i]+1))
    flag_23_data["DIVERSION_MONTHS"][i] = [month for month in months] 

for i in range(len(flag_23_data)):
    if (flag_23_data["DIRECT_SEASON_START_MONTH_1"][i] > flag_23_data["DIRECT_DIV_SEASON_END_MONTH_1"][i]):
        months = range(int(flag_23_data["DIRECT_SEASON_START_MONTH_1"][i]), 13) 
        months2 = range(1, int(flag_23_data["DIRECT_DIV_SEASON_END_MONTH_1"][i]))
        flag_23_data["DIVERSION_MONTHS"][i] = [month for month in months]
        months2 = [month for month in months2]
        for element in months2:
            flag_23_data["DIVERSION_MONTHS"][i].append(element)

flag_23_data["DIVERSION_MONTHS_2"] = [[]] * len(flag_23_data) 
for i in range(len(flag_23_data)):
    if (flag_23_data["DIRECT_SEASON_START_MONTH_2"][i] > flag_23_data["DIRECT_DIV_SEASON_END_MONTH_2"][i]):
        months = range(int(flag_23_data["DIRECT_SEASON_START_MONTH_2"][i]), 13) 
        months2 = range(1, int(flag_23_data["DIRECT_DIV_SEASON_END_MONTH_2"][i]))
    # int(flag_23_data["DIRECT_DIV_SEASON_END_MONTH_1"][i]+1))
        flag_23_data["DIVERSION_MONTHS_2"][i] = [month for month in months]
        months2 = [month for month in months2]
        for element in months2:
            flag_23_data["DIVERSION_MONTHS_2"][i].append(element)

flag_23_data["DIVERSION_MONTHS_3"] = [[]] * len(flag_23_data) 
for i in range(len(flag_23_data)):
    if (flag_23_data["DIRECT_SEASON_START_MONTH_3"][i] > flag_23_data["DIRECT_DIV_SEASON_END_MONTH_3"][i]):
        months = range(int(flag_23_data["DIRECT_SEASON_START_MONTH_3"][i]), 13) 
        months2 = range(1, int(flag_23_data["DIRECT_DIV_SEASON_END_MONTH_3"][i]))
    # int(flag_23_data["DIRECT_DIV_SEASON_END_MONTH_1"][i]+1))
        flag_23_data["DIVERSION_MONTHS_3"][i] = [month for month in months]
        months2 = [month for month in months2]
        for element in months2:
            flag_23_data["DIVERSION_MONTHS_3"][i].append(element)
# create month field names
months = ["DIRECT_DIV_JANUARY", "DIRECT_DIV_FEBRUARY", "DIRECT_DIV_MARCH", "DIRECT_DIV_APRIL", 
          "DIRECT_DIV_MAY", "DIRECT_DIV_JUNE", "DIRECT_DIV_JULY", "DIRECT_DIV_AUGUST", 
          "DIRECT_DIV_SEPTEMBER", "DIRECT_DIV_OCTOBER", "DIRECT_DIV_NOVEMBER", "DIRECT_DIV_DECEMBER"]
for month in months:
    flag_23_data[month] = "NA"
# check each month against list
# THIS LOOP TAKES AT LEAST 5 MINUTES !!!!!!!!
for j, month in enumerate(months):
    for i in range(len(flag_23_data)):
        flag_23_data[month][i] = np.where(
            ((j+1) in flag_23_data["DIVERSION_MONTHS"][i])  |
            ((j+1) in flag_23_data["DIVERSION_MONTHS_2"][i])  |
            ((j+1) in flag_23_data["DIVERSION_MONTHS_3"][i]),
            "Y", "N")





# merge data, etc.


rms_monthly = imp.import_rms_monthly(100000000000000)
rms_annual = imp.import_rms_annual(10000000000000)

rms_annual.head(1000)
flag_23_data.head(1000)
rms_monthly.head(1000)







flag_6a_data["RIPARIAN"] = np.where( 
    (flag_6a_data["SUB_TYPE"] == "RIPERIAN,")                    |  
    (flag_6a_data["SUB_TYPE"] == "RIPERIAN,OTHER,")              | 
    (flag_6a_data["SUB_TYPE"] == "RIPERIAN,COURTADJ,")           | 
    (flag_6a_data["SUB_TYPE"] == "RIPERIAN,PENDING,")            |
    (flag_6a_data["SUB_TYPE"] == "RIPERIAN,COURTADJ,OTHER,")     |
    (flag_6a_data["SUB_TYPE"] == "RIPERIAN,PENDING,OTHER,")      |   
    (flag_6a_data["SUB_TYPE"] == "RIPERIAN,PRE1914,")                    |  
    (flag_6a_data["SUB_TYPE"] == "RIPERIAN,PRE1914,OTHER,")              | 
    (flag_6a_data["SUB_TYPE"] == "RIPERIAN,PRE1914,COURTADJ,")           | 
    (flag_6a_data["SUB_TYPE"] == "RIPERIAN,PRE1914,PENDING,")            |
    (flag_6a_data["SUB_TYPE"] == "RIPERIAN,PRE1914,COURTADJ,OTHER,")     |
    (flag_6a_data["SUB_TYPE"] == "RIPERIAN,PRE1914,PENDING,OTHER,"),
    "Y", "N")



# df = pd.DataFrame(
#     {'trial_num': [1, 2, 3, 1, 2, 3],
#      'subject': [1, 1, 1, 2, 2, 2],
#      'samples': [list(np.random.randn(3).round(2)) for i in range(6)]
#     }
# )


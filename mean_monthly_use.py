# -*- coding: utf-8 -*-
"""
Created on Fri May 15 12:27:57 2020

@author: dp
"""

import pandas as pd
import calendar
import numpy as np

# RESULTS OF QA/QC should be used to clean up the rms dataset 

# rms reporting data
# rms = pd.read_csv("data/water_use_report.csv", nrows= 10000, low_memory=False)
rms = pd.read_csv("data/water_use_report.csv", low_memory=False)
rms["MONTH"] = rms['MONTH'].apply(lambda x: calendar.month_abbr[x])

rms = rms.fillna("DIRECT")

# mean monthly reported usage by year type
year_types = pd.read_csv("data/WATER_YEAR_TYPES_CLEAN.csv")
rms = rms.join(year_types.set_index("YEAR"), on = "YEAR", how = "left", rsuffix = "_type")
# rms.drop_duplicates(subset=["WATER_RIGHT_ID", "APPL_ID"], keep= "first", inplace=True)
rms.columns

# definitions
defs = {"W": "Wet year type",
        "AN": "Above normal year type",
        "BN": "Below normal year type",
        "D": "Dry year type",
        "C": "Critical year type"}

month = {1:'Janauary',
		 2:'February',
		 3:'March',
	 	 4:'April',
		 5:'May',
		 6:'June',
		 7:'July',
		 8:'August',
		 9:'September',
		 10:'October',
		 11:'November',
		 12:'December'}


group_year_types = rms.groupby(["APPL_ID", "MONTH", "DIVERSION_TYPE", "WATER_YEAR_TYPE"]).mean()
group_year_types.reset_index(inplace = True)
group_year_types = group_year_types.drop(["WATER_RIGHT_ID", "YEAR"], axis = 1) 
group_year_types.columns

# add all years value
# Mean monthly demand, all years
group_mm = rms.groupby(["APPL_ID", "MONTH", "DIVERSION_TYPE"]).mean()
group_mm["WATER_YEAR_TYPE"] = "ALL"
group_mm.reset_index(inplace = True)
group_mm = group_mm.drop(["WATER_RIGHT_ID", "YEAR"], axis = 1) 
group_mm.columns

# concatenate
all_groups = pd.concat([group_year_types, group_mm])

# create a dataframe for the multiIndex
index_df = pd.DataFrame({"APPL_ID" : all_groups["APPL_ID"], 
                         "DIVERSION_TYPE" : all_groups["DIVERSION_TYPE"],
                         "MONTH" : all_groups["MONTH"],
                         "WATER_YEAR_TYPE" : all_groups["WATER_YEAR_TYPE"]})
# define multiIndex
idx = pd.MultiIndex.from_frame(index_df)

# create the output dataframe
use_year_types_DF = pd.DataFrame({"MEAN_MONTHLY_REPORTED_USE" : all_groups["AMOUNT"]})
use_year_types_DF.set_index(idx, inplace = True)

unstacked_use_year_types = use_year_types_DF.unstack()




# group_mm.rename(columns={'AMOUNT':'MM_ALL_YEARS'}, inplace=True)
# group_mm = group_mm.drop(["WATER_RIGHT_ID", "YEAR"], axis = 1)


unstacked_use_year_types.to_csv("data/output/mean_monthly_reported_use.csv")

                                





















del(rms)

# flat file water rights info
# wr = pd.read_csv("data/ewrims_flat_file.csv", nrows= 100000, low_memory=False)
wr = pd.read_csv("data/ewrims_flat_file.csv", low_memory=False)
names = wr.columns

fv = wr[['APPLICATION_NUMBER','WATER_RIGHT_TYPE', "FACE_VALUE_AMOUNT", "FACE_VALUE_UNITS", "HUC_12_NUMBER", "USE_CODE", "PRIORITY_DATE", "MAX_RATE_OF_DIVERSION", "MAX_RATE_OF_DIV_UNIT"]]
del(wr)
fv.drop_duplicates(subset="APPLICATION_NUMBER", keep= "first", inplace=True)

# Mean monthly demand, all years
combined_mm = group_mm.merge(fv, how= "left", left_on='APPL_ID', right_on='APPLICATION_NUMBER')

# add some fields

use_types = ["Irrigation", "Municipal", "Domestic", "Power"] 
for use in use_types:
    combined_mm.loc[combined_mm["USE_CODE"] == use, "USE_CODE"] = use

size_types = ["< 10 AF", "10 - 100 AF", "100 - 1000", "> 1000 AF"]
combined_mm.loc[combined_mm["AMOUNT"] < 10, "SIZE"] = size_types[0]
combined_mm.loc[(combined_mm['AMOUNT'] >= 10) & (combined_mm['AMOUNT'] <= 100), "SIZE" ] = size_types[1]
combined_mm.loc[(combined_mm['AMOUNT'] > 100) & (combined_mm['AMOUNT'] < 1000), "SIZE" ] = size_types[2]
combined_mm.loc[combined_mm["AMOUNT"] > 1000, "SIZE"] = size_types[3]

# Mean Monthly Reported Use
combined_mm.to_csv("data/output/water_rights_data.csv")




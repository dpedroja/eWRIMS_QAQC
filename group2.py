# -*- coding: utf-8 -*-
"""
Created on Tue Feb 23 13:20:05 2021

@author: dpedroja
"""


import pandas as pd
import numpy as np
import import_functions as imp

# Flag 21: Verify if accurate reporting when reported "USE" is same value as "STORAGE”
# read in data
flat_data = imp.import_flat_file()
flag_21_data = pd.read_csv("eWRIMS_data/water_use_report.csv", low_memory=False)
# remove NaN diversion types and amount = 0
both_div_types = flag_21_data[(flag_21_data["DIVERSION_TYPE"].notna()) & (flag_21_data["AMOUNT"]>0)]
# then drop DIVERSION_TYPE
both_div_types.drop("DIVERSION_TYPE", axis = 1, inplace = True)
# Group by application, year, month, and count on AMOUNT
both_div_types = both_div_types.groupby(by = ["WATER_RIGHT_ID", "APPL_ID", "YEAR", "MONTH"]).count()
# new Field STORAGE_EQUALS_USE
# value criteria
both_div_types["STORAGE_EQUALS_USE"] = np.where( 
    (both_div_types["AMOUNT"] > 1), 
    "Y", "N")     
# merge data back to RMS
flag_21_data = flag_21_data.merge(both_div_types, how = "left", left_on = ["WATER_RIGHT_ID", "APPL_ID", "YEAR", "MONTH"], right_on = ["WATER_RIGHT_ID", "APPL_ID", "YEAR", "MONTH"])
flag_21_data.drop("AMOUNT_y", axis = 1, inplace = True)








# Flag 2: Unit conversion error - Identify over-reported diversions due to unit conversion errors
#  read in data
flat_data = imp.import_flat_file()
rms_annual = imp.import_rms_annual(9999999999)
# merge data
flag_2_data = rms_annual.merge(flat_data, how= "left", left_on='APPL_ID', right_index = True) 

# value criteria
flag_2_data["POSSIBLE_CONV_ERROR_TIER_1"] = np.where( 
    (flag_2_data["AMOUNT"] >= 2*(flag_2_data["FACE_VALUE_AMOUNT"]))     &  
    (flag_2_data["AMOUNT"] <= 3*(flag_2_data["FACE_VALUE_AMOUNT"])),      
    "Y", "N")
flag_2_data["POSSIBLE_CONV_ERROR_TIER_2"] = np.where( 
    (flag_2_data["AMOUNT"] > 3*(flag_2_data["FACE_VALUE_AMOUNT"])),
    "Y", "N")

        
# MISCELLANEOUS

# calculate percentage of face value reported
flag_2_data["%_Of_FACE_VALUE"] = ((flag_2_data["AMOUNT"] / flag_2_data["FACE_VALUE_AMOUNT"])*100).round(1)

# calculate year over year change %
flag_2_data.sort_values(["APPL_ID","DIVERSION_TYPE", "YEAR"] , inplace = True, ascending=[True, False, True])
flag_2_data["ANNUAL_CHANGE"] = (flag_2_data.groupby(["APPL_ID", "DIVERSION_TYPE"])["AMOUNT"].apply(pd.Series.pct_change, fill_method='ffill')).round(3)
flag_2_data = flag_2_data.replace([np.inf, -np.inf], np.NaN)

##################################################################################################

# Flag 19: Flagging when a water right holder reports the same diversion more than once (div amount, etc.) 
# for multiple water rights or dividing total diversion for all rights, etc. 
# read in data
flat_data = imp.import_flat_file()
rms_monthly = imp.import_rms_monthly_direct(99999999)
# merge tables
flag_19_data = rms_monthly.merge(flat_data, how= "left", left_on='APPL_ID', right_index = True) 
# new field DUPLICATE_REPORT_MULT_RIGHTS 
# reset index
flag_19_data.reset_index(inplace = True)
# define data fields    
fields = ["APPL_ID", "APPLICATION_PRIMARY_OWNER", "YEAR", "ANNUAL_AMOUNT", "JANUARY_DIRECT", "FEBRUARY_DIRECT", "MARCH_DIRECT",
                            "APRIL_DIRECT", "MAY_DIRECT", "JUNE_DIRECT", "JULY_DIRECT", "AUGUST_DIRECT",
                            "SEPTEMBER_DIRECT", "OCTOBER_DIRECT", "NOVEMBER_DIRECT", "DECEMBER_DIRECT"]

flag_19_data = flag_19_data[fields]
flag_19_data = flag_19_data[flag_19_data["ANNUAL_AMOUNT"] > 0]
     

flag_19_data = flag_19_data.groupby(by = ["APPLICATION_PRIMARY_OWNER", "YEAR", "ANNUAL_AMOUNT", "JANUARY_DIRECT", "FEBRUARY_DIRECT", "MARCH_DIRECT",
                            "APRIL_DIRECT", "MAY_DIRECT", "JUNE_DIRECT", "JULY_DIRECT", "AUGUST_DIRECT",
                            "SEPTEMBER_DIRECT", "OCTOBER_DIRECT", "NOVEMBER_DIRECT", "DECEMBER_DIRECT"]).count()

flag_19_data["DUPE_REPORTS"] = np.where(
    (flag_19_data["APPL_ID"] > 1),
    "Y", "N")

flag_19_data.columns







flag_19_data.columns

# flag_19_data = flag_19_data[["APPLICATION_PRIMARY_OWNER","APPL_ID", "YEAR", "ANNUAL_AMOUNT"]]
# flag_19_data = flag_19_data.groupby(by = ["APPLICATION_PRIMARY_OWNER", "ANNUAL_AMOUNT"]).count()
# flag_19_data = flag_19_data.groupby(by = ["APPLICATION_PRIMARY_OWNER", "WATER_RIGHT_ID", "YEAR", "ANNUAL_AMOUNT"]).count()

# group
flag_19_data = flag_19_data.groupby(by = ["PARTY_ID","YEAR", "ANNUAL_AMOUNT", "JANUARY_DIV", "FEBRUARY_DIV", "MARCH_DIV",
                            "APRIL_DIV", "MAY_DIV", "JUNE_DIV", "JULY_DIV", "AUGUST_DIV",
                            "SEPTEMBER_DIV", "OCTOBER_DIV", "NOVEMBER_DIV", "DECEMBER_DIV"]).count().reset_index()


# flag_19_data.columns
# value criteria
flag_19_data["DUPLICATE_REPORT_MULT_RIGHTS "] = np.where(
    (flag_19_data["ANNUAL_AMOUNT"] > 0)                         &
    (flag_19_data["APPL_ID"] > 1),
    "Y", "N")


# test = flag_19_data[(flag_19_data["APPLICATION_PRIMARY_OWNER"] == "ZUCKERMAN-MANDEVILLE, INC.")] 


##################################################################################################



















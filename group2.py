# -*- coding: utf-8 -*-
"""
Created on Tue Feb 23 13:20:05 2021

@author: dpedroja
"""


import pandas as pd
import numpy as np
import import_functions as imp

flat_data = imp.import_flat_file()


# Flag 21: Verify if accurate reporting when reported "USE" is same value as "STORAGE”

# read in RMS data
rms = pd.read_csv("eWRIMS_data/water_use_report.csv", nrows= 10000, low_memory=False)
# rms = pd.read_csv("data/water_use_report.csv", low_memory=False)

flag_21_data = rms





# combine storage & direct diversion amounts
rms = rms.groupby(by = ["WATER_RIGHT_ID", "APPL_ID", "YEAR", "MONTH"]).sum()
# aggregate monthly use into annual use
rms_annual = rms.groupby(["WATER_RIGHT_ID", "APPL_ID", "YEAR"]).sum()
# del wr to save memory if desired
del(rms)

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









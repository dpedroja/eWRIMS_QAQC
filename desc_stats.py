# -*- coding: utf-8 -*-
"""
Created on Mon Apr 12 09:26:48 2021

@author: dpedroja
"""

import pandas as pd
import calendar
import numpy as np
import import_functions as imp

# RESULTS OF QA/QC should be used to clean up the rms dataset 

# water diversion, i.e., combined DIRECT + STORAGE by month
water_div_monthly = imp.import_rms_monthly(90000)
water_div_annual = imp.import_rms_annual(999999999)

# MEANS
# mean annual 
mean_annual_div = water_div_annual.groupby(["WATER_RIGHT_ID", "APPL_ID"]).mean().round(2)
mean_annual_div.drop(["YEAR"], axis = 1, inplace = True)
mean_annual_div.columns = ["MEAN_ANNUAL_DIV"]

# mean monthly
water_div_monthly.reset_index(inplace = True)
mean_monthly_div = water_div_monthly.groupby(["WATER_RIGHT_ID", "APPL_ID"]).mean().round(2)
mean_monthly_div.drop(["YEAR"], axis = 1, inplace = True)
mean_monthly_div.columns = ['MEAN_JANUARY_DIV', 'MEAN_FEBRUARY_DIV', 'MEAN_MARCH_DIV', 'MEAN_APRIL_DIV', 
                            'MEAN_MAY_DIV', 'MEAN_JUNE_DIV', 'MEAN_JULY_DIV', 'MEAN_AUGUST_DIV', 
                            'MEAN_SEPTEMBER_DIV', 'MEAN_OCTOBER_DIV', 'MEAN_NOVEMBER_DIV', 'MEAN_DECEMBER_DIV']

# mean monthly by WYT
year_types = pd.read_csv("eWRIMS_data/water_year_types.csv")
# definitions
defs = {"W": "WET",
        "AN": "ABOVE NORMAL",
        "BN": "BELOW NORMAL",
        "D": "DRY",
        "C": "CRITICAL"}
mean_monthly_div_WYT = water_div_monthly.join(year_types.set_index("YEAR"), on = "YEAR", how = "left", rsuffix = "_type")
mean_monthly_div_WYT = mean_monthly_div_WYT.groupby(["WATER_RIGHT_ID", "APPL_ID", "WATER_YEAR_TYPE"]).mean().round(2)
mean_monthly_div_WYT.drop(["YEAR"], axis = 1, inplace = True)

# mean monthly excluding reports with all 0 values





# MEDIANS

# HISTROGRAMS

# by face value

# by beneficial use type

# by water right type





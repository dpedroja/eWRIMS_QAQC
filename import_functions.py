# -*- coding: utf-8 -*-
"""
Created on Thu Feb 25 10:55:17 2021

@author: dpedroja
"""

def import_flat_file():
    import pandas as pd
    # Flat file fields in use
    flat_cols = ["WR_WATER_RIGHT_ID",
                 "APPLICATION_NUMBER", 
                 "WATER_RIGHT_TYPE", 
                 "FACE_VALUE_AMOUNT", 
                 "PRIORITY_DATE", 
                 "HUC_8_NUMBER", 
                 "POD_STATUS", 
                 "POD_COUNT", 
                 "LATITUDE", "LONGITUDE", "NORTH_COORD", "EAST_COORD", 
                 "YEAR_DIVERSION_COMMENCED", 
                 "SUB_TYPE", 
                 "APPLICATION_ACCEPTANCE_DATE", 
                 "APPLICATION_RECD_DATE", 
                 "USE_DIRECT_DIV_ANNUAL_AMOUNT",
                 "DIRECT_DIV_AMOUNT"]
    # read in data
    flat_data = pd.read_csv("eWRIMS_data/ewrims_flat_file.csv", usecols = flat_cols, low_memory = False)
    # # drop records with null APPLICATION_NUMBER
    flat_data.dropna(axis = 0, subset=["APPLICATION_NUMBER"], inplace=True)
    # set index to APPLICATION_NUMBER
    flat_data.set_index("APPLICATION_NUMBER", drop = True, inplace = True)
    return flat_data


def import_flat_file_season():
    import pandas as pd
    # Flat file season fields in use
    flat_season_cols = ["WR_WATER_RIGHT_ID" ,
                        "APPLICATION_NUMBER",
                        "USE_CODE", 
                        "USE_STORAGE_AMOUNT",
                        "DIRECT_SEASON_START_MONTH_1",
                        "DIRECT_DIV_SEASON_END_MONTH_1",
                        "DIRECT_SEASON_START_MONTH_2",
                        "DIRECT_DIV_SEASON_END_MONTH_2",
                        "DIRECT_SEASON_START_MONTH_3",
                        "DIRECT_DIV_SEASON_END_MONTH_3"]
                        
    # read in data
    season_use = pd.read_csv("eWRIMS_data/ewrims_flat_file_use_season.csv", usecols = flat_season_cols, low_memory = False)
    return season_use

	
	
	


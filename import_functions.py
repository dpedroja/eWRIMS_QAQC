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
                 "HUC_12_NUMBER",
                 "POD_STATUS", 
                 "POD_COUNT", 
                 "LATITUDE", "LONGITUDE", "NORTH_COORD", "EAST_COORD", 
                 "YEAR_DIVERSION_COMMENCED", 
                 "SUB_TYPE", 
                 "APPLICATION_ACCEPTANCE_DATE", 
                 "APPLICATION_RECD_DATE", 
                 "USE_DIRECT_DIV_ANNUAL_AMOUNT",
                 "DIRECT_DIV_AMOUNT",
                 "STORAGE_AMOUNT",
                 "APPLICATION_PRIMARY_OWNER",
                 "PARTY_ID"]
    # read in data
    flat_data = pd.read_csv("eWRIMS_data/ewrims_flat_file.csv", usecols = flat_cols, low_memory = False)
    # # drop records with null APPLICATION_NUMBER
    flat_data.dropna(axis = 0, subset=["APPLICATION_NUMBER"], inplace=True)
    # flat_data.drop_duplicates(subset=["APPLICATION_NUMBER"])
    flat_data = flat_data.drop_duplicates()
    # set index to APPLICATION_NUMBER
    flat_data.set_index("APPLICATION_NUMBER", drop = True, inplace = True)
    return flat_data

def import_flat_file_pod():
    import pandas as pd
    # Flat file season fields in use
    flat_pod_cols = ["WR_WATER_RIGHT_ID" ,
                        "APPLICATION_NUMBER",
                        "HUC_12_NUMBER",
                        "POD_ID",
                        "POD_NUMBER",
                        "POD_STATUS",
                        "USE_CODE",  
                        "PARTY_ID"]
    # read in data
    flat_pod = pd.read_csv("eWRIMS_data/ewrims_flat_file_pod.csv", usecols = flat_pod_cols, low_memory = False)
    return flat_pod



def import_flat_file_season():
    import pandas as pd
    # Flat file season fields in use
    flat_season_cols = ["WR_WATER_RIGHT_ID" ,
                        "APPLICATION_NUMBER",
                        "HUC_12_NUMBER",
                        "POD_ID",
                        "POD_NUMBER",
                        "POD_STATUS",
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

def import_rms(rows):
    import pandas as pd
    # read in RMS data
    rms = pd.read_csv("eWRIMS_data/water_use_report.csv", nrows = rows, low_memory=False)
    # combine storage & direct diversion amounts
    rms = rms.groupby(by = ["WATER_RIGHT_ID", "APPL_ID", "YEAR", "MONTH"]).sum()
    return rms

def import_rms_annual(rows):
    import pandas as pd
    # read in RMS data
    rms_annual = pd.read_csv("eWRIMS_data/water_use_report.csv", nrows = rows, low_memory=False)
    # combine storage & direct diversion amounts
    rms_annual = rms_annual.groupby(by = ["WATER_RIGHT_ID", "APPL_ID", "YEAR", "MONTH"]).sum()
    # aggregate monthly use into annual use
    rms_annual = rms_annual.groupby(["WATER_RIGHT_ID", "APPL_ID", "YEAR"]).sum()
    return rms_annual

def import_rms_monthly(rows):
    import pandas as pd
    # read in RMS data
    rms_monthly = pd.read_csv("eWRIMS_data/water_use_report.csv", nrows = rows, low_memory=False)
    # combine storage & direct diversion amounts
    rms_monthly = rms_monthly.groupby(by = ["WATER_RIGHT_ID", "APPL_ID", "YEAR", "MONTH"]).sum()
    rms_monthly = rms_monthly.unstack()
    rms_monthly.columns = rms_monthly.columns.droplevel()
    rms_monthly.columns = ["JANUARY_DIV", "FEBRUARY_DIV", "MARCH_DIV", "APRIL_DIV", 
                       "MAY_DIV", "JUNE_DIV", "JULY_DIV", "AUGUST_DIV", 
                       "SEPTEMBER_DIV", "OCTOBER_DIV", "NOVEMBER_DIV", "DECEMBER_DIV"]
    return rms_monthly

# imports monthly USE fields by APPLICATION_NUMBER and YEAR
def import_rms_monthly_use(rows):
    import pandas as pd
    # read in RMS data
    rms_monthly_values = pd.read_csv("eWRIMS_data/water_use_report.csv", nrows = rows, low_memory=False)
    # use
    rms_monthly_use = rms_monthly_values[rms_monthly_values["DIVERSION_TYPE"]=="USE"]
    rms_annual_amount = rms_monthly_use.groupby(["WATER_RIGHT_ID", "APPL_ID", "YEAR"]).sum()
    rms_monthly_use = rms_monthly_use.groupby(by = ["WATER_RIGHT_ID", "APPL_ID", "YEAR", "MONTH"]).sum()
    rms_monthly_use = rms_monthly_use.unstack()
    rms_monthly_use.columns = rms_monthly_use.columns.droplevel()
    rms_monthly_use.columns = ["JANUARY_USE", "FEBRUARY_USE", "MARCH_USE", "APRIL_USE", 
                       "MAY_USE", "JUNE_USE", "JULY_USE", "AUGUST_USE", 
                       "SEPTEMBER_USE", "OCTOBER_USE", "NOVEMBER_USE", "DECEMBER_USE"]
    rms_monthly_use["ANNUAL_AMOUNT"] = rms_annual_amount["AMOUNT"]
    return rms_monthly_use 

# imports monthly STORAGE fields by APPLICATION_NUMBER and YEAR
def import_rms_monthly_storage(rows):
    import pandas as pd
    # read in RMS data
    rms_monthly_values = pd.read_csv("eWRIMS_data/water_use_report.csv", nrows = rows, low_memory=False)
    # storage
    rms_monthly_storage = rms_monthly_values[rms_monthly_values["DIVERSION_TYPE"]=="STORAGE"]
    rms_annual_amount = rms_monthly_storage.groupby(["WATER_RIGHT_ID", "APPL_ID", "YEAR"]).sum()
    rms_monthly_storage = rms_monthly_storage.groupby(by = ["WATER_RIGHT_ID", "APPL_ID", "YEAR", "MONTH"]).sum()
    rms_monthly_storage = rms_monthly_storage.unstack()
    rms_monthly_storage.columns = rms_monthly_storage.columns.droplevel()
    rms_monthly_storage.columns = ["JANUARY_STORAGE", "FEBRUARY_STORAGE", "MARCH_STORAGE", "APRIL_STORAGE", 
                       "MAY_STORAGE", "JUNE_STORAGE", "JULY_STORAGE", "AUGUST_STORAGE", 
                       "SEPTEMBER_STORAGE", "OCTOBER_STORAGE", "NOVEMBER_STORAGE", "DECEMBER_STORAGE"]
    rms_monthly_storage["ANNUAL_AMOUNT"] = rms_annual_amount["AMOUNT"]
    return rms_monthly_storage

# imports monthly DIRECT fields by APPLICATION_NUMBER and YEAR
def import_rms_monthly_direct(rows):
    import pandas as pd
    # read in RMS data
    rms_monthly_values = pd.read_csv("eWRIMS_data/water_use_report.csv", nrows = rows, low_memory=False)
    # direct
    rms_monthly_direct = rms_monthly_values[rms_monthly_values["DIVERSION_TYPE"]=="DIRECT"]
    rms_annual_amount = rms_monthly_direct.groupby(["WATER_RIGHT_ID", "APPL_ID", "YEAR"]).sum()
    rms_monthly_direct = rms_monthly_direct.groupby(by = ["WATER_RIGHT_ID", "APPL_ID", "YEAR", "MONTH"]).sum()
    rms_monthly_direct = rms_monthly_direct.unstack()
    rms_monthly_direct.columns = rms_monthly_direct.columns.droplevel()
    rms_monthly_direct.columns = ["JANUARY_DIRECT", "FEBRUARY_DIRECT", "MARCH_DIRECT", "APRIL_DIRECT", 
                       "MAY_DIRECT", "JUNE_DIRECT", "JULY_DIRECT", "AUGUST_DIRECT", 
                       "SEPTEMBER_DIRECT", "OCTOBER_DIRECT", "NOVEMBER_DIRECT", "DECEMBER_DIRECT"]
    rms_monthly_direct["ANNUAL_AMOUNT"] = rms_annual_amount["AMOUNT"]
    return rms_monthly_direct


def import_rms_all(rows):
    import import_functions as imp
    # read in RMS data
    rms_monthly = imp.import_rms_monthly(99999999999999)
    rms_annual = imp.import_rms_annual(99999999999)
    rms_annual.index
    rms_annual.columns = ["ANNUAL_AMOUNT"]
    rms_all = rms_annual.merge(rms_monthly, left_index = True, right_index = True)
    return rms_all


def import_rms_monthly_mean():
    import pandas as pd
    # read in RMS data
    rms_monthly_mean = pd.read_csv("eWRIMS_data/water_use_report.csv", low_memory=False)
    # combine storage & direct diversion amounts
    rms_monthly_mean = rms_monthly_mean.groupby(by = ["WATER_RIGHT_ID", "APPL_ID", "YEAR", "MONTH"]).sum()
    rms_monthly_mean = rms_monthly_mean.unstack()
    rms_monthly_mean = rms_monthly_mean.groupby(by = ["WATER_RIGHT_ID", "APPL_ID"]).mean()
    rms_monthly_mean.columns = rms_monthly_mean.columns.droplevel()
    rms_monthly_mean.columns = ["JANUARY_MEAN_DIV", "FEBRUARY_MEAN_DIV", "MARCH_MEAN_DIV", "APRIL_MEAN_DIV", 
                        "MAY_MEAN_DIV", "JUNE_MEAN_DIV", "JULY_MEAN_DIV", "AUGUST_MEAN_DIV", 
                        "SEPTEMBER_MEAN_DIV", "OCTOBER_MEAN_DIV", "NOVEMBER_MEAN_DIV", "DECEMBER_MEAN_DIV"]
    return rms_monthly_mean


def import_rms_raw(rows):
    import pandas as pd
    rms_raw =  pd.read_csv("eWRIMS_data/water_use_report.csv", nrows = rows, low_memory=False)
    return rms_raw








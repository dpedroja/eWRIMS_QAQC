# -*- coding: utf-8 -*-
"""
Created on Mon Feb 22 15:47:05 2021

@author: dpedroja
"""

import pandas as pd
import numpy as np
import import_functions as imp

def suppress_sci(x):
    output =  f"{x:.9f}"
    return output

# Flag 1: Identify Duplicate PODs 
# read in data
flat_data = imp.import_flat_file()
# select records where POD_STATUS = Active
flag_1a_data = flat_data[flat_data["POD_STATUS"] == "Active"]

# Calculate distance matrices between PODs within each huc 8 with this package:
from scipy.spatial import distance_matrix

n = 0
lst=[]
app_list = []
for huc in flag_1a_data["HUC_8_NUMBER"].unique():
    n = n + 1
    print(n, huc)
    data = flag_1a_data[["HUC_8_NUMBER", "LATITUDE", "LONGITUDE"]][flag_1a_data["HUC_8_NUMBER"]==huc]
    dist = pd.DataFrame(distance_matrix(data.values, data.values), index=data.index, columns=data.index)
    cols = dist.index
    lst.append((pd.DataFrame(np.triu(dist, k = 1), index = cols, columns = cols)).replace(0, 999999999))
    # df = pd.DataFrame(np.triu(dist, k = 1), index = cols, columns = cols).replace(0, 999999999)

x = 0
for i, list_ in enumerate(lst):
    x = x + 1
    print(x)
    df = lst[i]
    for i, app in enumerate(df.index):
        print(i,app)
        df1 = df[df.loc[df.index[i]] < 0.0005]
        app_list.append(df1.index.values)
        print(x)

result = set().union(*app_list)

# flat_data.loc["H502390"]

#################

dist_mat_1 = lst[133]
dist_mat_2 = lst[10]
dist_mat_3 = lst[84]
dist_mat_4 = lst[2]
dist_mat_5 = lst[138]

# test 
distance = dist_mat_5.loc["S008776", "S009004"]
lat1, long1 = flag_1a_data[["LATITUDE", "LONGITUDE"]].loc["S008776"]
lat2, long2 = flag_1a_data[["LATITUDE", "LONGITUDE"]].loc["S009004"]



flat_data["HUC_8_NUMBER"].loc["S008776"]


##########################################################################################

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
# Flag 4: Apportion total reported diversion for multiple POD's under a single application.  
# read in data

# read in data
flat_file_pod = imp.import_flat_file_pod()
# filter for active PODs
flag_4_data_active = flat_file_pod[flat_file_pod["POD_STATUS"] == "Active"]
# select only duplicated columns for groupby
flag_4_data_active = flag_4_data_active[["WR_WATER_RIGHT_ID", "APPLICATION_NUMBER", "HUC_12_NUMBER"]]
# drop duplicates (this removes all records wher multiple PODs are in the same HUC12 )
flag_4_data_unique = flag_4_data_active.drop_duplicates()
# count number of HUC12s for each Application number
flag_4_data_huc_count = flag_4_data_unique.groupby(["WR_WATER_RIGHT_ID", "APPLICATION_NUMBER"]).count()
# define new field MULTI_ACTIVE_PODS
flag_4_data_huc_count["MULTI_ACTIVE_PODS"] = np.where( 
    (flag_4_data_huc_count["HUC_12_NUMBER"] > 1),    
    "Y", "N")

# Record counts?
# merge to what file?














##########################################################################################


# Flag 6a: Identifying riparian and pre-1914 water rights and assigning a priority date 
# read in data
flat_data = imp.import_flat_file()
flag_6a_data = flat_data
# unique values of SUB_TYPE (for reference)
vals_sub_type = flag_6a_data["SUB_TYPE"].unique()

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

flag_6a_data["PRE1914"] = np.where( 
    (flag_6a_data["SUB_TYPE"] == "PRE1914,")                    |  
    (flag_6a_data["SUB_TYPE"] == "PRE1914,OTHER,")              | 
    (flag_6a_data["SUB_TYPE"] == "PRE1914,COURTADJ,")           | 
    (flag_6a_data["SUB_TYPE"] == "PRE1914,PENDING,")            |
    (flag_6a_data["SUB_TYPE"] == "PRE1914,COURTADJ,OTHER,")     |
    (flag_6a_data["SUB_TYPE"] == "PRE1914,PENDING,OTHER,")      |
    (flag_6a_data["SUB_TYPE"] == "RIPERIAN,PRE1914,")                    |  
    (flag_6a_data["SUB_TYPE"] == "RIPERIAN,PRE1914,OTHER,")              | 
    (flag_6a_data["SUB_TYPE"] == "RIPERIAN,PRE1914,COURTADJ,")           | 
    (flag_6a_data["SUB_TYPE"] == "RIPERIAN,PRE1914,PENDING,")            |
    (flag_6a_data["SUB_TYPE"] == "RIPERIAN,PRE1914,COURTADJ,OTHER,")     |
    (flag_6a_data["SUB_TYPE"] == "RIPERIAN,PRE1914,PENDING,OTHER,"),
    "Y", "N")
           

flag_6a_data["ASSIGNED_PRIORITY_DATE"] = np.where(
    (flag_6a_data["YEAR_DIVERSION_COMMENCED"].isna()),     
    10000000, flag_6a_data["PRIORITY_DATE"])


##########################################################################################

# Flag 6b
# data
flag_6b_data = flat_data

# unique values of SUB_TYPE
vals_water_right_type = flag_6b_data["WATER_RIGHT_TYPE"].unique()

# value criteria for POST_1914_APPROPRIATIVE
flag_6b_data["POST_1914_APPROPRIATIVE"] = np.where(
    (flag_6b_data["WATER_RIGHT_TYPE"] == "Appropriative")             |  
    (flag_6b_data["WATER_RIGHT_TYPE"] == "Registration Domestic")     |
    (flag_6b_data["WATER_RIGHT_TYPE"] == "Registration Irrigation")   |
    (flag_6b_data["WATER_RIGHT_TYPE"] == "Registration Livestock")    |
    (flag_6b_data["WATER_RIGHT_TYPE"] == "Stockpond"),   
    "Y", "N")

# ???????????????? flag_6b_data["POST_1914_PRIORITY_DATE"] = flag_6b_data["PRIORITY_DATE"]

# ASSIGNED_PRIORITY_DATE & PRIORITY_DATE_SOURCE
flag_6b_data["ASSIGNED_PRIORITY_DATE"] = flag_6b_data["PRIORITY_DATE"]
flag_6b_data["PRIORITY_DATE_SOURCE"] = "PRIORITY_DATE"

flag_6b_data["ASSIGNED_PRIORITY_DATE"] = np.where(
    (flag_6b_data["PRIORITY_DATE"].isna())              &
    (flag_6b_data["APPLICATION_RECD_DATE"].isna())      &
    (flag_6b_data["APPLICATION_ACCEPTANCE_DATE"].isna()),
    "99999999", flag_6b_data["PRIORITY_DATE"])

flag_6b_data["PRIORITY_DATE_SOURCE"] = np.where(
    (flag_6b_data["PRIORITY_DATE"].isna())              &
    (flag_6b_data["APPLICATION_RECD_DATE"].isna())      &
    (flag_6b_data["APPLICATION_ACCEPTANCE_DATE"].isna()),
    "99999999", "PRIORITY_DATE")

flag_6b_data["ASSIGNED_PRIORITY_DATE"] = np.where(
    (flag_6b_data["PRIORITY_DATE"].isna())                  &
    (flag_6b_data["APPLICATION_RECD_DATE"].isna())          &
    (flag_6b_data["APPLICATION_ACCEPTANCE_DATE"].notna())   &
    (flag_6b_data["ASSIGNED_PRIORITY_DATE"] != "99999999")    ,
    flag_6b_data["APPLICATION_ACCEPTANCE_DATE"], flag_6b_data["PRIORITY_DATE"])    

flag_6b_data["PRIORITY_DATE_SOURCE"] = np.where(
    (flag_6b_data["PRIORITY_DATE"].isna())                  &
    (flag_6b_data["APPLICATION_RECD_DATE"].isna())          &
    (flag_6b_data["APPLICATION_ACCEPTANCE_DATE"].notna())   &
    (flag_6b_data["ASSIGNED_PRIORITY_DATE"] != "99999999")    ,
    "APPLICATION_ACCEPTANCE_DATE", "PRIORITY_DATE")    

flag_6b_data["ASSIGNED_PRIORITY_DATE"] = np.where(
    (flag_6b_data["PRIORITY_DATE"].isna())                   &
    (flag_6b_data["APPLICATION_RECD_DATE"].notna())          &
    (flag_6b_data["ASSIGNED_PRIORITY_DATE"] != "99999999")     &   
    (flag_6b_data["ASSIGNED_PRIORITY_DATE"] != flag_6b_data["APPLICATION_ACCEPTANCE_DATE"]) ,
    flag_6b_data["APPLICATION_RECD_DATE"], flag_6b_data["PRIORITY_DATE"])    

flag_6b_data["PRIORITY_DATE_SOURCE"] = np.where(
    (flag_6b_data["PRIORITY_DATE"].isna())                   &
    (flag_6b_data["APPLICATION_RECD_DATE"].notna())          &
    (flag_6b_data["ASSIGNED_PRIORITY_DATE"] != "99999999")     &   
    (flag_6b_data["ASSIGNED_PRIORITY_DATE"] != flag_6b_data["APPLICATION_ACCEPTANCE_DATE"]) ,
    "APPLICATION_RECD_DATE", "PRIORITY_DATE")   

            
'Temporary Permit', 
'Appropriative (State Filing)', 
'Cert of Right - Power', 
'Federal Claims',
'Federal Stockponds', 
'Groundwater Recordation',
'Statement of Div and Use',
'Section 12 File', 
'Waste Water Change', 
'Not Determined',
'Adjudicated', 
'Non Jurisdictional', 
'Registration Cannabis'

# function to suppress scientific notation (for convenience)

def suppress_sci(x):
    output =  f"{x:.9f}"
    return output

##########################################################################################

# Flag 7: Identify water rights with Non-Consumptive Uses to account for water use returning water diverted back to the system
# read in data
flat_file_data = imp.import_flat_file()
flat_file_season = imp.import_flat_file_season()
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










##########################################################################################
# Flag 8: Identify incomplete or missing contact information in eWRIMS 
# Flat file party fields in use
flat_party_cols = ["CONTACT_INFORMATION_PHONE", "CONTACT_INFORMATION_EMAIL"]
# read in data
flat_party_data = pd.read_csv("eWRIMS_data/ewrims_flat_file_party.csv", usecols = flat_party_cols)

flag_8_data = flat_party_data

# number of character criteria
# define function that counts characters and returns Y if less than 9
def count_char(string):
    x = len(string)
    y = np.where(x<9, "Y", "N")
    return y
flag_8_data["MISSING_PHONE_NUMBER"] = flag_8_data["CONTACT_INFORMATION_PHONE"].astype(str).apply(count_char)
# value criteria
flag_8_data["MISSING_PHONE_NUMBER"] = np.where( 
    (flag_8_data["CONTACT_INFORMATION_PHONE"] == "999-999-9999")    |  
    (flag_8_data["CONTACT_INFORMATION_PHONE"] == "xxx-xxx-xxxx")    | 
    (flag_8_data["CONTACT_INFORMATION_PHONE"] == "NONE" )           | 
    (flag_8_data["CONTACT_INFORMATION_PHONE"] == "000-000-0000")    |
    (flag_8_data["MISSING_PHONE_NUMBER"] == "Y"), 
    "Y", "N")


##########################################################################################

# Flag 14: Classify records into tiers by diversion volumes (or other statistics)

# read in data 
flat_data = imp.import_flat_file()
flag_14_data = flat_data
# new fields: DIV_OVER_FACE_VALUE_AMOUNT and DIV_EXCEEDS_FACE_VALUE
# value criteria for DIV_EXCEEDS_FACE_VALUE
flag_14_data["DIV_EXCEEDS_FACE_VALUE"] = np.where(
    (flag_14_data["WATER_RIGHT_TYPE"] == "Appropriative")            |  
    (flag_14_data["FACE_VALUE_AMOUNT"].notna())                      &  
    (flag_14_data["DIRECT_DIV_AMOUNT"] > flag_14_data["FACE_VALUE_AMOUNT"]),          
    "Y", "N")

# value criteria for DIV_OVER_FACE_VALUE_AMOUNT
flag_14_data["DIV_OVER_FACE_VALUE_AMOUNT"] = np.where(
    (flag_14_data["WATER_RIGHT_TYPE"] == "Appropriative")            |  
    (flag_14_data["FACE_VALUE_AMOUNT"].notna())                      &
    (flag_14_data["DIRECT_DIV_AMOUNT"].notna())                      &
    (flag_14_data["DIRECT_DIV_AMOUNT"] > flag_14_data["FACE_VALUE_AMOUNT"]),          
    flag_14_data["DIRECT_DIV_AMOUNT"] - flag_14_data["FACE_VALUE_AMOUNT"], "NA")









##############################################################
# Flag 17: Classify records into groups by “Beneficial Use Types” - identify dominant BU (by sorting or other logic) when multiple BU’s are listed
# read in data
flat_file_data = imp.import_flat_file()
flat_file_season = imp.import_flat_file_season()
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
    1, 0)
# select subset of fields
flag_17_data = flag_17_data[['WR_WATER_RIGHT_ID', 'APPLICATION_NUMBER','AESTHETIC', 'AQUACULTURE', 'DOMESTIC', 'DUST_CONTROL', 'FIRE_PROTECTION', 'FISH_AND_WILDLIFE_PRESERVATION_AND_ENHANCEMENT', 'FROST_PROTECTION', 'HEAT_CONTROL', 'INCIDENTAL_POWER', 'INDUSTRIAL', 'IRRIGATION', 'MILLING', 'MINING', 'MUNICIPAL', 'OTHER', 'POWER', 'RECREATIONAL', 'SNOW_MAKING', 'STOCKWATERING', 'WATER_QUALITY']]
# combine use types
flag_17_data = flag_17_data.groupby(by = ["WR_WATER_RIGHT_ID", "APPLICATION_NUMBER"]).sum()
# replace 1/0 with Y/N
flag_17_data.replace((1, 0), ("Y", "N"), inplace=True)

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

##################################################################################################

# Flag 20. Duplicate values reported for multiple months or multiple years 

# read in data
rms_raw = imp.import_rms_raw(999999999)

flag_20_data = rms_raw

div_types = flag_20_data["DIVERSION_TYPE"].unique()

flag_20_DIRECT = flag_20_data[flag_20_data["DIVERSION_TYPE"] == "DIRECT"]
flag_20_USE = flag_20_data[flag_20_data["DIVERSION_TYPE"] == "USE"]
flag_20_STORAGE = flag_20_data[flag_20_data["DIVERSION_TYPE"] == "STORAGE"]
flag_20_Combined = flag_20_data[flag_20_data["DIVERSION_TYPE"] == "Combined (Direct + Storage)"]

flag_20_DIRECT = flag_20_DIRECT.drop(["MONTH"], axis = 1)
flag_20_DIRECT = flag_20_DIRECT[flag_20_DIRECT["AMOUNT"] > 0].groupby(["WATER_RIGHT_ID", "APPL_ID", "YEAR", "AMOUNT"]).count()

flag_20_DIRECT["DUPLICATE_MONTHLY_REPORTING"] = np.where(
    (flag_20_DIRECT["DIVERSION_TYPE"] == 12),
    "Y", "N")



# Number of Non-zero months
# flag_20_DIRECT = flag_20_DIRECT[flag_20_DIRECT["AMOUNT"] > 0].groupby(["WATER_RIGHT_ID", "APPL_ID", "YEAR"]).count()

# flag_20_DIRECT["NUM_NON_0_MO"] = np.where(
#     flag_20_DIRECT[flag_20_DIRECT["AMOUNT"] > 0].groupby(["WATER_RIGHT_ID", "APPL_ID", "YEAR", "AMOUNT"]).count()
#     )









##################################################################################################

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

##################################################################################################


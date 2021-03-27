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
#n is just a counter
n = 0
lst=[]
for huc in flag_1a_data["HUC_8_NUMBER"].unique():
    n = n + 1
    print(n, huc)
    data = flag_1a_data[["HUC_8_NUMBER", "LATITUDE", "LONGITUDE"]][flag_1a_data["HUC_8_NUMBER"]==huc]
    dist = pd.DataFrame(distance_matrix(data.values, data.values), index=data.index, columns=data.index)
    cols = dist.index
    lst.append((pd.DataFrame(np.triu(dist, k = 1), index = cols, columns = cols)).replace(0, 999999999))

threshold = 0.0005
app_list = []
x = 0
for i, list_ in enumerate(lst):
    x = x + 1
    print(x)
    df = lst[i]
    for j, app in enumerate(df.index):
        print(j,app)
        df1 = df[df.loc[df.index[j]] < threshold]
        if len(df1) > 0:
            # app_list.append(tuple((df.index[j], df1.index.values[0])))
            app_list.append(tuple((df.index[j], df1.index.values[0],  df.loc[df.index[j],df1.index.values[0]])))
        print(x)
# convert tuple to dataframe
df = pd.DataFrame(app_list, columns=["APPLICATION_NUMBER_1", "APPLICATION_NUMBER_2", "DISTANCE"])
# separate temporarily
df_APP_1 = df[["APPLICATION_NUMBER_1", "DISTANCE"]]
df_APP_2 = df[["APPLICATION_NUMBER_2", "DISTANCE"]]
flag_1a_data.reset_index(inplace = True)
# select subset of flat_file fields
flag_1a_data[['WR_WATER_RIGHT_ID', 'APPLICATION_NUMBER', 'POD_COUNT','LATITUDE', 'LONGITUDE', 'HUC_12_NUMBER', 'HUC_8_NUMBER']]
# merge everything together
df_APP_1 = df_APP_1.merge(flag_1a_data, how = "left", left_on = "APPLICATION_NUMBER_1", right_on = "APPLICATION_NUMBER")
df_APP_2 = df_APP_2.merge(flag_1a_data, how = "left", left_on = "APPLICATION_NUMBER_2", right_on = "APPLICATION_NUMBER")
flag_1_data = df_APP_1.merge(df_APP_2, left_index = True, right_index = True, suffixes = ("_1", "_2"))
# write output to .csv
flag_1_data.to_csv("output\\f1_duplicate_pods.csv")

#################
# stuff for testing 
'''
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

'''

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

flag_2_data.to_csv("output\\f2_unit_conv_error.csv")
        
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

flag_4_data_huc_count.to_csv("output\\f4_pods_by_huc.csv")
'''
##########################################################################################


# Flag 6: Assign Priority Date 
# read in data
flat_data = imp.import_flat_file()
flag_6_data = flat_data
# define new fields
flag_6_data["ASSIGNED_PRIORITY_DATE"] = "NA"
flag_6_data["ASSIGNED_PRIORITY_DATE_SOURCE"] = "NA"
# populate WATER_RIGHT_TYPES Statements of Div and Use and Federal Claims
# unique values of WATER_RIGHT_TYPE (for reference)
vals_sub_type = flag_6_data["WATER_RIGHT_TYPE"].unique()
# select WATER_RIGHT_TYPE "Statement of Div and Use" and "Federal Claims"
flag_6_a = flag_6_data[(flag_6_data["WATER_RIGHT_TYPE"] == "Statement of Div and Use") | 
                       (flag_6_data["WATER_RIGHT_TYPE"] == "Federal Claims") ]

flag_6_a["ASSIGNED_PRIORITY_DATE"] = np.where(
    (
     (flag_6_a["SUB_TYPE"] == "PRE1914,") |
     (flag_6_a["SUB_TYPE"] == "PRE1914,COURTADJ,")
     )   &
    (flag_6_a["YEAR_DIVERSION_COMMENCED"].isna()),
    11111111, "NA")

flag_6_a["ASSIGNED_PRIORITY_DATE"] = np.where(
    (flag_6_a["ASSIGNED_PRIORITY_DATE"] != 11111111) &
    (flag_6_a["YEAR_DIVERSION_COMMENCED"] <= 1914),
    (flag_6_a["YEAR_DIVERSION_COMMENCED"]*10000 + 101), 10000000)

flag_6_a["ASSIGNED_PRIORITY_DATE_SOURCE"] = np.where(
    (
     (flag_6_a["SUB_TYPE"] == "PRE1914,") |
     (flag_6_a["SUB_TYPE"] == "PRE1914,COURTADJ,"))  &
    (flag_6_a["YEAR_DIVERSION_COMMENCED"].isna()),
    "PRE1914 DEFAULT", "RIPARIAN DEFAULT")

flag_6_a["ASSIGNED_PRIORITY_DATE_SOURCE"] = np.where(
    (flag_6_a["ASSIGNED_PRIORITY_DATE"] != 11111111) &
    (flag_6_a["YEAR_DIVERSION_COMMENCED"] <= 1914),
    flag_6_a["YEAR_DIVERSION_COMMENCED"], "NA")

##################################################
# WATER_RIGHT_TYPE for everything else
flag_6_data = flat_data
flag_6_b = flag_6_data[(flag_6_data["WATER_RIGHT_TYPE"] != "Statement of Div and Use") & 
                       (flag_6_data["WATER_RIGHT_TYPE"] != "Federal Claims") ]

flag_6_b["ASSIGNED_PRIORITY_DATE"] = np.where(
    (flag_6_b["PRIORITY_DATE"].isna()) &
    (flag_6_b["RECEIPT_DATE"].isna()) &
    (flag_6_b["APPLICATION_RECD_DATE"].isna()) &
    (flag_6_b["APPLICATION_ACCEPTANCE_DATE"].isna()),
    99999999, "NA")
    
flag_6_b = flag_6_b[["PRIORITY_DATE", "RECEIPT_DATE", "APPLICATION_RECD_DATE", "APPLICATION_ACCEPTANCE_DATE"]]
# fill NaNs with a random date in far in the future 
flag_6_b = flag_6_b.fillna(("01/01/2100 12:00:00 AM"))

# dates = flag_6_b[["PRIORITY_DATE", "RECEIPT_DATE", "APPLICATION_RECD_DATE", "APPLICATION_ACCEPTANCE_DATE"]]
# dates = dates.fillna(("01/01/2100 12:00:00 AM"))

# convert dates to datetime data type
import datetime as dt
flag_6_b["PRIORITY_DATE"] = pd.to_datetime(flag_6_b["PRIORITY_DATE"]).dt.date
flag_6_b["RECEIPT_DATE"] = pd.to_datetime(flag_6_b["RECEIPT_DATE"]).dt.date
flag_6_b["APPLICATION_RECD_DATE"] = pd.to_datetime(flag_6_b["APPLICATION_RECD_DATE"]).dt.date
flag_6_b["APPLICATION_ACCEPTANCE_DATE"] = pd.to_datetime(flag_6_b["APPLICATION_ACCEPTANCE_DATE"]).dt.date
# take minimum value
flag_6_b["MIN"] = flag_6_b.min(axis = 1)


# flag_6_b[flag_6_b["ASSIGNED_PRIORITY_DATE"] == "01/01/2100 12:00:00 AM"].columns

flag_6_b[
    flag_6_b[[
        "PRIORITY_DATE", "RECEIPT_DATE", "APPLICATION_RECD_DATE", "APPLICATION_ACCEPTANCE_DATE"
        ]]
    == flag_6_b["MIN"]
    
    ].columns[0:5]

df[df[‘Name’]==’Donna’].index.values


flag_6_b["MIN_label"] = flag_6_b.idxmin(axis = "columns")

# get column of min value






# assigned it except where 99999999 is assigned
flag_6_b["ASSIGNED_PRIORITY_DATE"] = "NA"
flag_6_b["ASSIGNED_PRIORITY_DATE"] = np.where(
    (flag_6_b["ASSIGNED_PRIORITY_DATE"] == "NA"),
    flag_6_b["MIN"], "NA")


#########
    flag_6_b["ASSIGNED_PRIORITY_DATE_SOURCE"] = np.where(
    (flag_6_b["ASSIGNED_PRIORITY_DATE"] == 99999999),
    "UNKNOWN DEFAULT", 

############

'''
# WRONG date !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
WR_ID
45088
APP_NUMBER G193521
'''


'''
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

'''

##########################################################################################

# Flag 7: Return Flows by Beneficial Use Type 
# read in data
flat_file_data = imp.import_flat_file()
flat_file_season = imp.import_flat_file_season()
# merge data
flag_7_data = flat_file_season.merge(flat_file_data, how = "left", on = "WR_WATER_RIGHT_ID")
# unique values of USE_CODE (for reference)
vals_sub_type = flag_7_data["USE_CODE"].unique()

# new field: USE_RETURN_JAN, etc.

lookup = pd.read_csv("eWRIMS_data\\flag_7_17_lookup.csv")

flag_7_data = flag_7_data[["APPLICATION_NUMBER", "USE_CODE"]]
flag_7_data = flag_7_data.merge(lookup, how = "left", left_on = "USE_CODE", right_on = "USE_CODE")

flag_7_data.to_csv("output\\f7_water_use_return.csv")

##########################################################################################
# Flag 8: Identify incomplete or missing contact information in eWRIMS 
# Flat file party fields in use
flat_party_cols = ["APPLICATION_NUMBER", "PARTY_ID", "PARTY_NAME", "CONTACT_INFORMATION_PHONE", "CONTACT_INFORMATION_EMAIL"]
# read in data
flat_party_data = pd.read_csv("eWRIMS_data/ewrims_flat_file_party.csv", usecols = flat_party_cols)
# flat_party_data = pd.read_csv("eWRIMS_data/ewrims_flat_file_party.csv")
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

flag_8_data.columns

flag_8_data.to_csv("output\\f8_missing_contact_info.csv")


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
flat_file_season = flat_file_season[['WR_WATER_RIGHT_ID', 'APPLICATION_NUMBER', 'USE_CODE']] 
flat_file_season = flat_file_season.drop_duplicates()
# merge data
flag_17_data = flat_file_season.merge(flat_file_data, how = "left", on = "WR_WATER_RIGHT_ID")
# Beneficial use types
vals = sorted(flag_17_data["USE_CODE"].unique())
# create field headings
fields = []
for i, item in enumerate(vals):
    fields.append(vals[i].replace(" ", "_").upper())
# Assign 1 if use code is listed
for i, use in enumerate(vals):
    flag_17_data[fields[i]] = np.where( 
    (flag_17_data["USE_CODE"] == vals[i]),               
    1, 0)
# select subset of fields
flag_17a_data = flag_17_data[["WR_WATER_RIGHT_ID", "APPLICATION_NUMBER", "USE_CODE", "WATER_RIGHT_TYPE", "PRIMARY_OWNER_ENTITY_TYPE", "AESTHETIC", "AQUACULTURE", "DOMESTIC", "DUST_CONTROL", "FIRE_PROTECTION", "FISH_AND_WILDLIFE_PRESERVATION_AND_ENHANCEMENT", "FROST_PROTECTION", "HEAT_CONTROL", "INCIDENTAL_POWER", "INDUSTRIAL", "IRRIGATION", "MILLING", "MINING", "MUNICIPAL", "OTHER", "POWER", "RECREATIONAL", "SNOW_MAKING", "STOCKWATERING", "WATER_QUALITY"]]
# combine use types into 1 record
flag_17a_data = flag_17_data.groupby(by = ["WR_WATER_RIGHT_ID", "APPLICATION_NUMBER"]).sum()
# replace 1/0 with Y/N
flag_17a_data.replace((1, 0), ("Y", "N"), inplace=True)
# output 17a to .csv
flag_17a_data.to_csv("output\\f17a_beneficial_uses.csv")  
    
# 17b1  Identify Primary Beneficial Use 
# select subset of fields
flag_17b1_data = flag_17_data[["WR_WATER_RIGHT_ID", "APPLICATION_NUMBER", "USE_CODE", "WATER_RIGHT_TYPE", "PRIMARY_OWNER_ENTITY_TYPE", "AESTHETIC", "AQUACULTURE", "DOMESTIC", "DUST_CONTROL", "FIRE_PROTECTION", "FISH_AND_WILDLIFE_PRESERVATION_AND_ENHANCEMENT", "FROST_PROTECTION", "HEAT_CONTROL", "INCIDENTAL_POWER", "INDUSTRIAL", "IRRIGATION", "MILLING", "MINING", "MUNICIPAL", "OTHER", "POWER", "RECREATIONAL", "SNOW_MAKING", "STOCKWATERING", "WATER_QUALITY"]]
# create more field temporary headings for 17b
fields = []
for i, item in enumerate(vals):
    fields.append(vals[i].replace(" ", "_"))
# get lookup table of rankings
lookup = pd.read_csv("eWRIMS_data\\flag_7_17_lookup.csv")
# define dictionary
lookup_dict = pd.Series(lookup.USE_RANKING.values,index=lookup.USE_CODE).to_dict()
# loop to assign ranking or 0 depending on use 
for i, use in enumerate(vals):
    flag_17b1_data[fields[i]] = np.where( 
    (flag_17b1_data["USE_CODE"] == vals[i]),               
    lookup_dict[vals[i]], 0)  
flag_17b1_data = flag_17b1_data[['WR_WATER_RIGHT_ID', 'APPLICATION_NUMBER',
        'Aesthetic', 'Aquaculture', 'Domestic', 'Dust_Control',
       'Fire_Protection', 'Fish_and_Wildlife_Preservation_and_Enhancement',
       'Frost_Protection', 'Heat_Control', 'Incidental_Power', 'Industrial',
       'Irrigation', 'Milling', 'Mining', 'Municipal', 'Other', 'Power',
       'Recreational', 'Snow_Making', 'Stockwatering', 'Water_Quality']] 
# consolidate in one record per APPLICATION_NUMBER
flag_17b1_data = flag_17b1_data.groupby(by = ["WR_WATER_RIGHT_ID", "APPLICATION_NUMBER"]).sum()
# replace 0s with a large number so the minumum will choose the highest rank (1)
flag_17b1_data.replace(0, 10000, inplace=True)

# take highest rank
flag_17b1_data["TOP_RANK"] = flag_17b1_data.min(axis = 1)
# lookup dictionary reversed
lookup_dict2 = pd.Series(lookup.USE_CODE.values,index=lookup.USE_RANKING).to_dict()
# convert ranking to use
flag_17b1_data["PRIMARY_USE"] = "NA"
for i, app in enumerate(flag_17b1_data["TOP_RANK"]):
    flag_17b1_data["PRIMARY_USE"].iloc[i] = lookup_dict2[flag_17b1_data["TOP_RANK"].iloc[i]]
# save the above until later

# 17b2  Identify Primary Beneficial Use 
# values of fields for reference
wr_types = flag_17_data["WATER_RIGHT_TYPE"].unique()
entity_types = flag_17_data["PRIMARY_OWNER_ENTITY_TYPE"].unique()
# select subset of fields
flag_17b2_data = flag_17_data[["WR_WATER_RIGHT_ID", "APPLICATION_NUMBER", "USE_CODE", "WATER_RIGHT_TYPE", "PRIMARY_OWNER_ENTITY_TYPE", "AESTHETIC", "AQUACULTURE", "DOMESTIC", "DUST_CONTROL", "FIRE_PROTECTION", "FISH_AND_WILDLIFE_PRESERVATION_AND_ENHANCEMENT", "FROST_PROTECTION", "HEAT_CONTROL", "INCIDENTAL_POWER", "INDUSTRIAL", "IRRIGATION", "MILLING", "MINING", "MUNICIPAL", "OTHER", "POWER", "RECREATIONAL", "SNOW_MAKING", "STOCKWATERING", "WATER_QUALITY"]]
flag_17b2_data["PRIMARY_USE"] = "NA"
# define PRIMARY_USE and value criteria
flag_17b2_data["PRIMARY_USE"] = np.where(
    ((flag_17b2_data["WATER_RIGHT_TYPE"] == "Appropriative")  |
    (flag_17b2_data["WATER_RIGHT_TYPE"] == "Federal Claims")  |
    (flag_17b2_data["WATER_RIGHT_TYPE"] == "Statement of Div and Use")) &
    ((flag_17b2_data["IRRIGATION"] == "Y") &
    (flag_17b2_data["MUNICIPAL"] == "Y"))                    |
    ((flag_17b2_data["WATER_RIGHT_TYPE"] == "Appropriative")  |
    (flag_17b2_data["WATER_RIGHT_TYPE"] == "Federal Claims")  |
    (flag_17b2_data["WATER_RIGHT_TYPE"] == "Statement of Div and Use")) &
    ((flag_17b2_data["IRRIGATION"] == "Y") &
    (flag_17b2_data["DOMESTIC"] == "Y")  &
    (flag_17b2_data["PRIMARY_OWNER_ENTITY_TYPE"] == "Government (State/Municipal)")), 
    "IRR_MUN", "NOT INCLUDED")

flag_17b2_data["PRIMARY_USE"] = np.where(
    ((flag_17b2_data["WATER_RIGHT_TYPE"] == "Stockpond")  |
    (flag_17b2_data["WATER_RIGHT_TYPE"] == "Federal Stockponds")  |
    (flag_17b2_data["WATER_RIGHT_TYPE"] == "Registration Livestock"))  &
    (flag_17b2_data["PRIMARY_USE"] != "IRR_MUN"),
    "STOCKWATERING", flag_17b2_data["PRIMARY_USE"])

flag_17b2_data["PRIMARY_USE"] = np.where(
    ((flag_17b2_data["WATER_RIGHT_TYPE"] == "Registration Irrigation")  |    
    (flag_17b2_data["WATER_RIGHT_TYPE"] == "Registration Cannabis")) &
    ((flag_17b2_data["PRIMARY_USE"] != "IRR_MUN") |
     (flag_17b2_data["PRIMARY_USE"] != "STOCKWATERING")),
     "IRRIGATION", flag_17b2_data["PRIMARY_USE"])

flag_17b2_data["PRIMARY_USE"] = np.where(
    (flag_17b2_data["WATER_RIGHT_TYPE"] == "Registration Domestic")  &
    ((flag_17b2_data["PRIMARY_USE"] != "IRR_MUN") |
     (flag_17b2_data["PRIMARY_USE"] != "STOCKWATERING")  |
     (flag_17b2_data["PRIMARY_USE"] != "IRRIGATION")),
     "DOMESTIC", flag_17b2_data["PRIMARY_USE"])

flag_17b2_data = flag_17b2_data[['WR_WATER_RIGHT_ID', 'APPLICATION_NUMBER',
       'WATER_RIGHT_TYPE', 'PRIMARY_OWNER_ENTITY_TYPE',
       'PRIMARY_USE']].drop_duplicates()
# Select records for assigned PRIMARY_USE by ranking
flag_17b_merge = flag_17b2_data[flag_17b2_data["PRIMARY_USE"] == "NOT INCLUDED"]
# set other records aside
flag_17b2_data = flag_17b2_data[flag_17b2_data["PRIMARY_USE"] != "NOT INCLUDED"]

# dont need PRIMARY_USE or WR_WATER_RIGHT_ID
flag_17b_merge = flag_17b_merge[["APPLICATION_NUMBER", "WATER_RIGHT_TYPE",
       "PRIMARY_OWNER_ENTITY_TYPE"]]
# reset index so can base merge on APPLICATION_NUMBER
flag_17b1_data.reset_index(inplace = True)
# merge
flag_17b = flag_17b_merge.merge(flag_17b1_data, how = "left", left_on = "APPLICATION_NUMBER", right_on = "APPLICATION_NUMBER")
# Select fields
flag_17b = flag_17b[['WR_WATER_RIGHT_ID', 'APPLICATION_NUMBER', 'WATER_RIGHT_TYPE', 
    'PRIMARY_OWNER_ENTITY_TYPE', 'PRIMARY_USE']]
# concatenate lists with each PRIMARY_USE values
flag_17 = pd.concat([flag_17b, flag_17b2_data], axis = 0)                      
# define EQUAL_USES
flag_17["EQUAL_USES"] = "NA"
flag_17["EQUAL_USES"] = np.where(
    (flag_17["PRIMARY_USE"] == "IRR_MUN"),
    "Y", "N")
flag_17.to_csv("output\\f17b_primary_use_type.csv")


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
# unmodified water_use_report
flag_20_data = rms_raw
# obtain unique values of DIVERSION_TYPE
div_types = flag_20_data["DIVERSION_TYPE"].unique()
# separate by DIVERSION_TYPE
flag_20_DIRECT = flag_20_data[flag_20_data["DIVERSION_TYPE"] == "DIRECT"]
flag_20_USE = flag_20_data[flag_20_data["DIVERSION_TYPE"] == "USE"]
flag_20_STORAGE = flag_20_data[flag_20_data["DIVERSION_TYPE"] == "STORAGE"]
flag_20_Combined = flag_20_data[flag_20_data["DIVERSION_TYPE"] == "Combined (Direct + Storage)"]

flag_20_DIRECT = flag_20_DIRECT.drop(["MONTH"], axis = 1)
flag_20_DIRECT = flag_20_DIRECT[flag_20_DIRECT["AMOUNT"] > 0].groupby(["WATER_RIGHT_ID", "APPL_ID", "YEAR", "AMOUNT"]).count()
flag_20_USE = flag_20_USE.drop(["MONTH"], axis = 1)
flag_20_USE = flag_20_USE[flag_20_USE["AMOUNT"] > 0].groupby(["WATER_RIGHT_ID", "APPL_ID", "YEAR", "AMOUNT"]).count()
flag_20_STORAGE = flag_20_STORAGE.drop(["MONTH"], axis = 1)
flag_20_STORAGE = flag_20_STORAGE[flag_20_STORAGE["AMOUNT"] > 0].groupby(["WATER_RIGHT_ID", "APPL_ID", "YEAR", "AMOUNT"]).count()
flag_20_Combined = flag_20_Combined.drop(["MONTH"], axis = 1)
flag_20_Combined = flag_20_Combined[flag_20_Combined["AMOUNT"] > 0].groupby(["WATER_RIGHT_ID", "APPL_ID", "YEAR", "AMOUNT"]).count()

flag_20_DIRECT["DUPLICATE_MONTHLY_REPORTING"] = np.where(
    (flag_20_DIRECT["DIVERSION_TYPE"] == 12),
    "Y", "N")
flag_20_DIRECT = flag_20_DIRECT.reset_index()
flag_20_DIRECT = flag_20_DIRECT[["APPL_ID", "DUPLICATE_MONTHLY_REPORTING"]][flag_20_DIRECT["DUPLICATE_MONTHLY_REPORTING"]=="Y"]

flag_20_USE["DUPLICATE_MONTHLY_REPORTING"] = np.where(
    (flag_20_USE["DIVERSION_TYPE"] == 12),
    "Y", "N")
flag_20_USE = flag_20_USE.reset_index()
flag_20_USE = flag_20_USE[["APPL_ID", "DUPLICATE_MONTHLY_REPORTING"]][flag_20_USE["DUPLICATE_MONTHLY_REPORTING"]=="Y"]

flag_20_STORAGE["DUPLICATE_MONTHLY_REPORTING"] = np.where(
    (flag_20_STORAGE["DIVERSION_TYPE"] == 12),
    "Y", "N")
flag_20_STORAGE = flag_20_STORAGE.reset_index()
flag_20_STORAGE = flag_20_STORAGE[["APPL_ID", "DUPLICATE_MONTHLY_REPORTING"]][flag_20_STORAGE["DUPLICATE_MONTHLY_REPORTING"]=="Y"]

flag_20_Combined["DUPLICATE_MONTHLY_REPORTING"] = np.where(
    (flag_20_Combined["DIVERSION_TYPE"] == 12),
    "Y", "N")
flag_20_Combined = flag_20_Combined.reset_index()
flag_20_Combined = flag_20_Combined[["APPL_ID", "DUPLICATE_MONTHLY_REPORTING"]][flag_20_Combined["DUPLICATE_MONTHLY_REPORTING"]=="Y"]

flag_20_data = pd.concat([flag_20_DIRECT, flag_20_USE, flag_20_STORAGE, flag_20_Combined])
flag_20_data = flag_20_data.drop_duplicates()

rms_annual = imp.import_rms_annual(999999999)
rms_annual.reset_index(inplace = True, drop = True)
flag_20_annual = rms_annual[rms_annual["AMOUNT"] > 0].groupby(["WATER_RIGHT_ID", "APPL_ID", "AMOUNT"]).count()
flag_20_annual.columns

flag_20_annual["NUMBER_OF_REPEATED_REPORTS"] = np.where(
    (flag_20_annual["YEAR"] > 1),
    flag_20_annual["YEAR"], 0
    )
flag_20_annual.drop(["YEAR"], axis = 1, inplace = True)

flag_20_annual.to_csv("output\\f20_repeat_dup_reports.csv")

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

flag_21_data.to_csv("output\\f21_duplicate_storage_use.csv")

##################################################################################################

# this routine supports flags 22 and 23

# first get length of diversion season in months (A)

# read in data
flat_file_season = imp.import_flat_file_season()
flag_22_data = flat_file_season
flat_file_data = imp.import_flat_file()

# replace NaN with 0
flag_22_data = flag_22_data.fillna(0)
# define diversion seasons
season_start = ["DIRECT_SEASON_START_MONTH_1", "DIRECT_SEASON_START_MONTH_2", "DIRECT_SEASON_START_MONTH_3"]
season_end = ["DIRECT_DIV_SEASON_END_MONTH_1", "DIRECT_DIV_SEASON_END_MONTH_2", "DIRECT_DIV_SEASON_END_MONTH_3"]
# define fields with lists as values
div_fields = ["DIVERSION_MONTHS", "DIVERSION_MONTHS_2", "DIVERSION_MONTHS_3"]
for field in div_fields:
    flag_22_data[field] = [[]] * len(flag_22_data) 

# populate list of lists of months
# TAKES A FEW MINUTES
for k, field in enumerate(season_start):
    for i in range(len(flag_22_data)):
        months = range(int(flag_22_data[season_start[k]][i]), 
        int(flag_22_data[season_end[k]][i]+1))
        flag_22_data[div_fields[k]][i] = [month for month in months] 
    for i in range(len(flag_22_data)):
        print(k,i)
        if (flag_22_data[season_start[k]][i] > flag_22_data[season_end[k]][i]):
            months = range(int(flag_22_data[season_start[k]][i]), 13) 
            months2 = range(1, int(flag_22_data[season_end[k]][i]))
            flag_22_data[div_fields[k]][i] = [month for month in months]
            months2 = [month for month in months2]
            for element in months2:
                flag_22_data[div_fields[k]][i].append(element)
# define new field DIV_SEASON_LENGTH
flag_22_data["DIV_SEASON_LENGTH"]= "NA"            
# length of diversion season in months
for i, app in enumerate(flag_22_data["POD_ID"]):
    a = set(flag_22_data["DIVERSION_MONTHS"][i])
    a.discard(0)
    b = set(flag_22_data["DIVERSION_MONTHS_2"][i])
    b.discard(0)
    c = set(flag_22_data["DIVERSION_MONTHS_3"][i])
    c.discard(0)
    d = (a.union(b)).union(c)
    flag_22_data["DIV_SEASON_LENGTH"][i] = len(d)

# get face value amount from flat file & merge
flag_22_data = flag_22_data.merge(flat_file_data, how = "left", on = "WR_WATER_RIGHT_ID")

# calculate monthly amount as FACE_VALUE_AMOUNT divided by DIV_SEASON_LENGTH calculated above
flag_22_data["FV_PER_MONTH"] = [np.divide(flag_22_data["FACE_VALUE_AMOUNT"][i], flag_22_data["DIV_SEASON_LENGTH"][i], 
              out=np.zeros_like(flag_22_data["FACE_VALUE_AMOUNT"][i]), where=flag_22_data["DIV_SEASON_LENGTH"][i]!=0) 
     for i, right in enumerate(flag_22_data["FACE_VALUE_AMOUNT"])]

# create month field names
months = ["DIRECT_DIV_JANUARY", "DIRECT_DIV_FEBRUARY", "DIRECT_DIV_MARCH", "DIRECT_DIV_APRIL", 
          "DIRECT_DIV_MAY", "DIRECT_DIV_JUNE", "DIRECT_DIV_JULY", "DIRECT_DIV_AUGUST", 
          "DIRECT_DIV_SEPTEMBER", "DIRECT_DIV_OCTOBER", "DIRECT_DIV_NOVEMBER", "DIRECT_DIV_DECEMBER"]
for month in months:
    flag_22_data[month] = "NA"
# check each month against list
# COULD TAKE A WHILE (5 MINUTES OR MORE)
for j, month in enumerate(months):
    for i in range(len(flag_22_data)):
        print(j,i)
        flag_22_data[month][i] = np.where(
            ((j+1) in flag_22_data["DIVERSION_MONTHS"][i])  |
            ((j+1) in flag_22_data["DIVERSION_MONTHS_2"][i])  |
            ((j+1) in flag_22_data["DIVERSION_MONTHS_3"][i]),
            "1", "0")

# FLAG 22 loop

# define field names
monthly_dist_fv = ["JANUARY_DISTRIBUTION_FV", "FEBRUARY_DISTRIBUTION_FV", "MARCH_DISTRIBUTION_FV", "APRIL_DISTRIBUTION_FV", 
          "MAY_DISTRIBUTION_FV", "JUNE_DISTRIBUTION_FV", "JULY_DISTRIBUTION_FV", "AUGUST_DISTRIBUTION_FV", 
          "SEPTEMBER_DISTRIBUTION_FV", "OCTOBER_DISTRIBUTION_FV", "NOVEMBER_DISTRIBUTION_FV", "DECEMBER_DISTRIBUTION_FV"]
for field_name in monthly_dist_fv:
    flag_22_data[field_name] = "NA"
# define (January) distribution as DIRECT_DIV_JAN * FV_PER_MONTH
for k, month in enumerate(monthly_dist_fv):
    flag_22_data[monthly_dist_fv[k]] = (np.array(flag_22_data["FV_PER_MONTH"], dtype = float) * np.array(flag_22_data[months[k]], dtype = float)).round(3)

flag_22_data.to_csv("output\\f22_apportion_fv.csv")
# Flag 22 end

# Flag 23: Flag water rights with reported diversions outside of diversion season
# select a subset of fields
div_season_data = pd.DataFrame(flag_22_data[["WR_WATER_RIGHT_ID", "APPLICATION_NUMBER", "USE_CODE",
        "DIRECT_DIV_JANUARY", "DIRECT_DIV_FEBRUARY", "DIRECT_DIV_MARCH", "DIRECT_DIV_APRIL", 
        "DIRECT_DIV_MAY", "DIRECT_DIV_JUNE", "DIRECT_DIV_JULY", "DIRECT_DIV_AUGUST",
        "DIRECT_DIV_SEPTEMBER", "DIRECT_DIV_OCTOBER", "DIRECT_DIV_NOVEMBER", "DIRECT_DIV_DECEMBER"]])
# change back to Y / N data
div_season_data = div_season_data.replace("0", "N")
div_season_data = div_season_data.replace("1", "Y")
# import rms monthly data
rms_monthly = imp.import_rms_monthly(100000000000000)
rms_monthly.reset_index(inplace = True)
# merge data
flag_23_data = rms_monthly.merge(div_season_data, how = "inner", left_on = "APPL_ID", right_on = "APPLICATION_NUMBER")
# this merge multiplies a lot of records because of multiple use types per application number. 

# rms monthly reported diversion fields
div_reported = ["JANUARY_DIV", "FEBRUARY_DIV", "MARCH_DIV", "APRIL_DIV", 
              "MAY_DIV", "JUNE_DIV", "JULY_DIV", "AUGUST_DIV", 
              "SEPTEMBER_DIV", "OCTOBER_DIV", "NOVEMBER_DIV", "DECEMBER_DIV"]
# Field DIV_REPORTED_OUTSIDE_DIV_SEASON criteria
for n, month in enumerate(div_reported):
    flag_23_data["DIV_REPORTED_OUTSIDE_DIV_SEASON"] = np.where( 
        (flag_23_data[div_reported[n]] > 0)                          &  
        (flag_23_data[months[n]] == "N"),
        "Y", "N")

flag_23_data = flag_23_data[['WATER_RIGHT_ID', 'APPL_ID', 'YEAR', 
                 'DIRECT_DIV_JANUARY', 'DIRECT_DIV_FEBRUARY', 'DIRECT_DIV_MARCH',
                 'DIRECT_DIV_APRIL', 'DIRECT_DIV_MAY', 'DIRECT_DIV_JUNE',
                 'DIRECT_DIV_JULY', 'DIRECT_DIV_AUGUST', 'DIRECT_DIV_SEPTEMBER',
                 'DIRECT_DIV_OCTOBER', 'DIRECT_DIV_NOVEMBER', 'DIRECT_DIV_DECEMBER',
                 'DIV_REPORTED_OUTSIDE_DIV_SEASON']]

# number of records is greater than RMS because some applications have different diversion months for differenct use codes (apparently)
flag_23_data = flag_23_data.drop_duplicates()

flag_23_data.to_csv("output\\f23_div_out_of_season.csv")
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 24 08:18:53 2021

@author: dpedroja
"""



import pandas as pd
import numpy as np

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







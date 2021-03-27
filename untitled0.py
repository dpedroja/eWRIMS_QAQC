# -*- coding: utf-8 -*-
"""
Created on Sat Mar 27 12:43:33 2021

@author: dpedroja
"""

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
flat_data = imp.import_flat_file_pod()
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
            app_list.append(tuple((df.index[j], df1.index.values[0],  df.loc[df.index[j],df1.index.values[0] ]         )))
        print(x)

app_list[0]


flat_data["POD_ID"].loc[df1.index.values[0]]


################################
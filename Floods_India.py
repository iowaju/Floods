import urllib
import datetime
import time
import os
import numpy/
import pandas as pd
import csv
import h5py
import pyodbc
import datetime
import arcpy
from netCDF4 import Dataset
from datetime import time, timedelta, date


def processing():
    directory = "C:/Dev/floods/raw_data/"
    concat_df = pd.DataFrame()
    for files in os.listdir(directory):
        if files.endswith(".bin"):

            date = datetime.datetime.strptime(str(files[13:21]), '%Y%m%d').strftime('%Y-%m-%d')

            #loading the bin file
            x = numpy.fromfile(directory + files, dtype = 'f')

            #creating the numpy array and applying the mask to remove invalid values
            nrows, ncols = 800, 2458
            k = x.reshape(nrows, ncols)
            mask = (k == -9999)
            ma = numpy.ma.masked_array(k, mask = mask, dtype = "float64")

            #load the dataframe
            df = pd.DataFrame(ma)
            #print df
            #df_to_numeric = df.apply(pd.to_numeric)

            #organizing the dataframe
            df_unstack = df.unstack()
            df_unstack2 = df_unstack.to_frame()
            df_unstack3 = pd.DataFrame(df_unstack2.to_records())
            df_unstack_num = df_unstack3
            df_unstack_num.columns = ['long','lat','value']

            #reassign lat long values
            df_unstack_num['lat'] = df_unstack_num['lat'] * 0.125
            df_unstack_num['lat'] = df_unstack_num['lat'] - 50
            df_unstack_num['lat'] = df_unstack_num['lat'] * (-1)
            df_unstack_num['long'] = df_unstack_num['long'] * 0.125
            df_unstack_num['long'] = df_unstack_num['long'] - 127.25
            df_unstack_num.fillna(0, inplace = True)

            #cleaning and clipping the dataframe
            df = df_unstack_num.loc[df_unstack_num['value'] != 0 ]
            df = df[df.lat > 14]
            df = df[df.lat < 24]
            df = df[df.long > 78]
            df['date'] = date

            concat_df = pd.concat([concat_df, df])
    concat_df.to_csv(directory + "Complete.csv")

processing()

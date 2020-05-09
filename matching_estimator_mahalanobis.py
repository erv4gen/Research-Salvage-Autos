#import dependencies
import glob
import itertools
import json
import multiprocessing as mp
import os
import pickle
import re
import sys
import time
import warnings

import numpy as np
import pandas as pd
import scipy as sp
from scipy import stats
from scipy.spatial.distance import mahalanobis
from TempFolder.TempFolder import Temp
from tqdm import tqdm



  



warnings.filterwarnings('ignore')

with open('nogit\\path','r') as f:
    path_to_csv = f.read()


#set up path
Temp.set_path(path_to_csv +'SC-temp\\')


model_name = sys.argv[1]
# model_name = '20p_500k1m'

print('Model name: ',model_name)
#load input files
car_demo_joined = Temp.load_obj('car_demo_joined')



#load filtering results
match_null = Temp.load_obj('matched_candidates_'+model_name)

#filter out rows without match
match_e = match_null.loc[match_null.map(lambda x: len(x)>0)]


#expected match
columns_to_match = ['Actual_Cash_Value_adj','Odometer_Replace']

#calculate covariance
cov = np.cov(car_demo_joined[columns_to_match].dropna(how='any').values.T)

#inverse covariance
inv_covmat = np.linalg.inv(cov)

pct_match = ['Actual_Cash_Value_adj','Odometer_Replace']

compare_columns = pct_match + ['Price_Sold_or_Highest_Bid_adj']
def mahalanobis_distance(idx):
    '''
    The method calculates the Mahalanobis between matching value and candidates and select the
    closest ome
    '''
    #get vector to match
    sc_tm = car_demo_joined.loc[idx.name,columns_to_match].values
    
    #get candidates vectors 
    cand_ids = np.array([v for v in idx.values]).reshape(-1,)
    candidates = car_demo_joined.loc[cand_ids,columns_to_match].values
    
    #iterage over each candidate and calculate the distance between two vectors
    d = np.array([])
    for i in range(candidates.shape[0]):
        d= np.append( mahalanobis(sc_tm,candidates[i,:],inv_covmat) ,d)
    closest_match =cand_ids[d.argmin()]
    return car_demo_joined.loc[closest_match,compare_columns]

print('Calculating Mahalanobis distances ...')
mahalanobis_match = (match_e
                # .iloc[:10]
                .to_frame()
                .apply(mahalanobis_distance,axis=1)#.rename('mahalanobis_big_city')
                #.to_frame()
                .join(car_demo_joined[compare_columns],rsuffix='_matched')
                )



#save output
Temp.save_obj(mahalanobis_match,'mahalanobis_match_'+model_name)


# mahalanobis_match = Temp.load_obj('mahalanobis_match_'+model_name)

#calculate difference between small and the large cities
mahalanobis_match_diff = mahalanobis_match.diff(axis=1).dropna(axis=1)#.abs()

#calc statistics
print(mahalanobis_match_diff.agg(['mean','std']) ,'\nt-test p value:'
, round(stats.ttest_1samp(mahalanobis_match_diff.values.reshape(-1,),0)[1],3)
,'\n',mahalanobis_match.head())

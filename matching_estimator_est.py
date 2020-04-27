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
from scipy import stats
from TempFolder.TempFolder import Temp
from tqdm import tqdm



  

warnings.filterwarnings('ignore')

with open('nogit\\path','r') as f:
    path_to_csv = f.read()


#set up path
Temp.set_path(path_to_csv +'SC-temp\\')

#load input files
out_df = Temp.load_obj('out_df_working')
car_demo_joined = Temp.load_obj('car_demo_joined')



#label big cities
smalb = 5e5
big_b = 1e6
car_demo_joined.loc[car_demo_joined['CENSUS_closest']<smalb , 'is_big_city'] = 0
car_demo_joined.loc[car_demo_joined['CENSUS_closest']>big_b, 'is_big_city'] = 1
car_demo_joined = car_demo_joined.dropna(subset = ['is_big_city'])


model_name = '20p_500k1m'


match_within = .2
exact_match = ['Make','Model_short','Model_Age','Prim_Damage']
pct_match = ['Actual_Cash_Value_adj','Odometer_Replace']
print(
    'small county boundary:' , smalb, ' bigg county boundary', big_b,
    'Rows excluded from the analysis:',car_demo_joined.loc[car_demo_joined['is_big_city'].isna()].shape[0]
,'\nLooking for a match within ', match_within,' for ', pct_match
,'\nExact match: ', exact_match
)


def find_match_expected(small_c, big_c):
    '''
    Find expected match - for each row in the `small_c` (small county) dataset find ID of a matching values from the `big_c` dataset

    Params:
    small_c: pd.DataFrame
    big_c: pd.DataFrame

    Returns:
    pd.DataFrame
    '''
    def find_match_for_row(x):
        # try:
        bs = big_c.copy()
        # import pdb; pdb.set_trace()
        bs[pct_match[0]] = np.abs((x[pct_match[0]]/bs[pct_match[0]])-1)
        bs[pct_match[1]]  = np.abs((x[pct_match[1]]/bs[pct_match[1]])-1)
        bs = bs.fillna(9999)
        return bs.loc[
                        (bs[pct_match[0]]  <=match_within )
                        & (bs[pct_match[1]] <=match_within)
                        ].index.tolist()

        # except: return []
    # import pdb; pdb.set_trace()
    return small_c.apply(find_match_for_row,axis=1)
    

#Iterate over all dataframe and find matches
match_l = []
# i = 0 
for grp , df in tqdm(car_demo_joined.groupby(exact_match)):
    if len(df) <2:
        continue
    is_big_city_bs = df.loc[df.is_big_city == 1]
    is_small_city_df = df.loc[df.is_big_city == 0]

    match_l.append(find_match_expected(is_small_city_df,is_big_city_bs))
    # i+=1

    # if i>550:
    #     break

#concat to the dataframe
match_null = pd.concat(match_l).dropna(axis=1).iloc[:,0].rename('match')

#filter out rows without match
match_e = match_null.loc[match_null.map(lambda x: len(x)>0)]

#save calculation results
Temp.save_obj(match_null,'expected_match_indexed_'+model_name)

print('% of records that matched: ', 100*len(match_e) / len(match_null),'%\n',match_e.head())

#expected match

def expected_mean(idx):
    '''
    The method select the number of observations and calculates the average value across them
    '''
    return car_demo_joined.loc[idx,'Price_Sold_or_Highest_Bid_adj'].mean()

expected_match = (match_e.apply(expected_mean).rename('expected_value_big_city').to_frame()
                .join(car_demo_joined.Price_Sold_or_Highest_Bid_adj.rename('actual_value_small_city')))



#save output
Temp.save_obj(expected_match,'expected_match_'+model_name)


expected_match = Temp.load_obj('expected_match_'+model_name)

#calculate difference between small and the large cities
expected_match_diff = expected_match.diff(axis=1).dropna(axis=1)#.abs()

#calc statistics
print(expected_match_diff.agg(['mean','std']) ,'\nt-test p value:', round(stats.ttest_1samp(expected_match_diff.values.reshape(-1,),0)[1],3),'\n',expected_match.head())

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


from collections import namedtuple
  

warnings.filterwarnings('ignore')

with open('nogit\\path','r') as f:
    path_to_csv = f.read()


#set up path
Temp.set_path(path_to_csv +'SC-temp\\')

#load input files
exact_match = ['Make','Model_short','Model_Age','Prim_Damage']


ModelType = namedtuple('ModelType',['model_name','smalb','big_b','match_within','pct_match','truncated'])

models_pop = [
    ModelType('Pop_10p_500k1m_with_odm_no_trunc',5e5,1e6,.1,['Actual_Cash_Value_adj','Odometer_Replace'],False)
    ,ModelType('Pop_05p_500k1m_with_odm_no_trunc',5e5,1e6,.05,['Actual_Cash_Value_adj','Odometer_Replace'],False)
    ,ModelType('Pop_20p_500k1m_with_odm_no_trunc',5e5,1e6,.20,['Actual_Cash_Value_adj','Odometer_Replace'],False)


    ,ModelType('Pop_10p_1m1m_with_odm_trunc',1e6,1e6,.1,['Actual_Cash_Value_adj','Odometer_Replace'],True)
    ,ModelType('Pop_05p_1m1m_with_odm_trunc',1e6,1e6,.05,['Actual_Cash_Value_adj','Odometer_Replace'],True)
    ,ModelType('Pop_20p_1m1m_with_odm_trunc',1e6,1e6,.2,['Actual_Cash_Value_adj','Odometer_Replace'],True)

    ,ModelType('Pop_10p_500k1m_with_odm_trunc',5e5,1e6,.1,['Actual_Cash_Value_adj','Odometer_Replace'],True)
    ,ModelType('Pop_05p_500k1m_with_odm_trunc',5e5,1e6,.05,['Actual_Cash_Value_adj','Odometer_Replace'],True)
    ,ModelType('Pop_20p_500k1m_with_odm_trunc',5e5,1e6,.2,['Actual_Cash_Value_adj','Odometer_Replace'],True)




    ,ModelType('Pop_10p_1m1m_no_odm_trunc',1e6,1e6,.1,['Actual_Cash_Value_adj'],True)
    ,ModelType('Pop_05p_1m1m_no_odm_trunc',1e6,1e6,.05,['Actual_Cash_Value_adj'],True)
    ,ModelType('Pop_20p_1m1m_no_odm_trunc',1e6,1e6,.2,['Actual_Cash_Value_adj'],True)
    ,ModelType('Pop_05p_1m1m_no_odm_no_trunc',1e6,1e6,.05,['Actual_Cash_Value_adj'],False)

    ,ModelType('Pop_10p_5e51m_no_odm_trunc',5e5,1e6,.1,['Actual_Cash_Value_adj'],True)
    ,ModelType('Pop_05p_5e51m_no_odm_trunc',5e5,1e6,.05,['Actual_Cash_Value_adj'],True)
    ,ModelType('Pop_20p_5e51m_no_odm_trunc',5e5,1e6,.2,['Actual_Cash_Value_adj'],True)
    ,ModelType('Pop_05p_5e51m_no_odm_no_trunc',5e5,1e6,.05,['Actual_Cash_Value_adj'],False)
    
]


models_crime = [
    ModelType('Crime_10p_5k_with_odm_no_trunc',5e3,5e3,.1,['Actual_Cash_Value_adj','Odometer_Replace'],False)
    ,ModelType('Crime_05p_5k_with_odm_no_trunc',5e3,5e3,.05,['Actual_Cash_Value_adj','Odometer_Replace'],False)
    ,ModelType('Crime_20p_5k_with_odm_no_trunc',5e3,5e3,.20,['Actual_Cash_Value_adj','Odometer_Replace'],False)


    
    ,ModelType('Crime_10p_5k_with_odm_trunc',5e3,5e3,.1,['Actual_Cash_Value_adj','Odometer_Replace'],True)
    ,ModelType('Crime_05p_5k_with_odm_trunc',5e3,5e3,.05,['Actual_Cash_Value_adj','Odometer_Replace'],True)
    ,ModelType('Crime_20p_5k_with_odm_trunc',5e3,5e3,.2,['Actual_Cash_Value_adj','Odometer_Replace'],True)


    
    ,ModelType('Crime_10p_5e31m_no_odm_trunc',5e3,5e3,.1,['Actual_Cash_Value_adj'],True)
    ,ModelType('Crime_05p_5e31m_no_odm_trunc',5e3,5e3,.05,['Actual_Cash_Value_adj'],True)
    ,ModelType('Crime_20p_5e31m_no_odm_trunc',5e3,5e3,.2,['Actual_Cash_Value_adj'],True)
    ,ModelType('Crime_05p_5e31m_no_odm_no_trunc',5e3,5e3,.05,['Actual_Cash_Value_adj'],False)
    
]

models_unemp = [
    ModelType('Unemp_10p_5k_with_odm_no_trunc',5.,5.,.1,['Actual_Cash_Value_adj','Odometer_Replace'],False)
    ,ModelType('Unemp_05p_5k_with_odm_no_trunc',5.,5.,.05,['Actual_Cash_Value_adj','Odometer_Replace'],False)
    ,ModelType('Unemp_20p_5k_with_odm_no_trunc',5.,5.,.20,['Actual_Cash_Value_adj','Odometer_Replace'],False)


    
    ,ModelType('Unemp_10p_5k_with_odm_trunc',5.,5.,.1,['Actual_Cash_Value_adj','Odometer_Replace'],True)
    ,ModelType('Unemp_05p_5k_with_odm_trunc',5.,5.,.05,['Actual_Cash_Value_adj','Odometer_Replace'],True)
    ,ModelType('Unemp_20p_5k_with_odm_trunc',5.,5.,.2,['Actual_Cash_Value_adj','Odometer_Replace'],True)


    
    ,ModelType('Unemp_10p_5.1m_no_odm_trunc',5.,5.,.1,['Actual_Cash_Value_adj'],True)
    ,ModelType('Unemp_05p_5.1m_no_odm_trunc',5.,5.,.05,['Actual_Cash_Value_adj'],True)
    ,ModelType('Unemp_20p_5.1m_no_odm_trunc',5.,5.,.2,['Actual_Cash_Value_adj'],True)
    ,ModelType('Unemp_05p_5.1m_no_odm_no_trunc',5.,5.,.05,['Actual_Cash_Value_adj'],False)
]

models_income = [
    ModelType('Income_10p_5k_with_odm_no_trunc',5e4,5e4,.1,['Actual_Cash_Value_adj','Odometer_Replace'],False)
    ,ModelType('Income_05p_5k_with_odm_no_trunc',5e4,5e4,.05,['Actual_Cash_Value_adj','Odometer_Replace'],False)
    ,ModelType('Income_20p_5k_with_odm_no_trunc',5e4,5e4,.20,['Actual_Cash_Value_adj','Odometer_Replace'],False)


    
    ,ModelType('Income_10p_5k_with_odm_trunc',5e4,5e4,.1,['Actual_Cash_Value_adj','Odometer_Replace'],True)
    ,ModelType('Income_05p_5k_with_odm_trunc',5e4,5e4,.05,['Actual_Cash_Value_adj','Odometer_Replace'],True)
    ,ModelType('Income_20p_5k_with_odm_trunc',5e4,5e4,.2,['Actual_Cash_Value_adj','Odometer_Replace'],True)


    
    ,ModelType('Income_10p_5e41m_no_odm_trunc',5e4,5e4,.1,['Actual_Cash_Value_adj'],True)
    ,ModelType('Income_05p_5e41m_no_odm_trunc',5e4,5e4,.05,['Actual_Cash_Value_adj'],True)
    ,ModelType('Income_20p_5e41m_no_odm_trunc',5e4,5e4,.2,['Actual_Cash_Value_adj'],True)
    ,ModelType('Income_05p_5e41m_no_odm_no_trunc',5e4,5e4,.05,['Actual_Cash_Value_adj'],False)
]


#run_on = 'CENSUS_closest'
# run_on = 'GRNDTOT_closest'
# run_on = 'Unemp_pct_closest'
run_on = 'Income_adj_closest'
models_dict = dict(CENSUS_closest = models_pop 
                    ,GRNDTOT_closest = models_crime
                    ,Unemp_pct_closest=models_unemp
                    ,Income_adj_closest = models_income
                    )
def main():
    for id, model in enumerate(models_dict[run_on]):

        car_demo_joined = Temp.load_obj('car_demo_joined')
        
        car_demo_joined.loc[car_demo_joined[run_on]<=model.smalb , 'group0'] = 0
        car_demo_joined.loc[car_demo_joined[run_on]>model.big_b, 'group0'] = 1
        car_demo_joined = car_demo_joined.dropna(subset = ['group0'])







        compare_columns = model.pct_match + ['Price_Sold_or_Highest_Bid_adj']
        #truncate quantiles
        original_size =car_demo_joined.shape[0]
        if model.truncated:
            for col_ in compare_columns:
                car_demo_joined = car_demo_joined.loc[car_demo_joined[col_] < car_demo_joined[col_].quantile(0.99) ]
            



        print('Model name: ',model.model_name,' #',id,' of ',len(models_dict[run_on]),' \n'
            ,'                                               small county boundary:' , model.smalb, ' bigg county boundary', model.big_b,
            '\nRows excluded from the analysis:',original_size - car_demo_joined.shape[0]
        ,'\nLooking for a match within ', model.match_within,' for ', model.pct_match
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
                bs[model.pct_match[0]] = np.abs((x[model.pct_match[0]]/bs[model.pct_match[0]])-1)
                
                if len(model.pct_match)>1:
                    bs[model.pct_match[1]]  = np.abs((x[model.pct_match[1]]/bs[model.pct_match[1]])-1)
                    bs = bs.fillna(9999)
                    bs = bs.loc[
                                (bs[model.pct_match[0]]  <=model.match_within )
                                & (bs[model.pct_match[1]]  <=model.match_within )
                                ].index.tolist()
                else:                                   
                    bs = bs.fillna(9999)
                    bs = bs.loc[
                                (bs[model.pct_match[0]]  <=model.match_within )
                                ].index.tolist()
                return bs

            return small_c.apply(find_match_for_row,axis=1)
            

        #Iterate over all dataframe and find matches
        match_l = []
        i = 0 
        for grp , df in tqdm(car_demo_joined.groupby(exact_match)):
            if len(df) <2:
                continue
            group0_df = df.loc[df.group0 == 1]
            group1_df = df.loc[df.group0 == 0]

            match_l.append(find_match_expected(group1_df,group0_df))
            i+=1

            # if i>550:
            #     break

        #concat to the dataframe
        match_null = pd.concat(match_l).dropna(axis=1).iloc[:,0].rename('match')

        #filter out rows without match
        match_e = match_null.loc[match_null.map(lambda x: len(x)>0)]

        #save calculation results
        Temp.save_obj(match_null,'matched_candidates_'+model.model_name)

        print('% of records matched: ', 100*len(match_e) / len(match_null)
        ,'# of records matched:', len(match_e)

        ,'%\n',match_e.head())

        #expected match


        


        def expected_mean_f(idx):
            '''
            The method select the number of observations and calculates the average value across them
            '''
            return car_demo_joined.loc[idx,compare_columns].mean()

        expected_mean = (match_e.apply(expected_mean_f)
                        .join(car_demo_joined[compare_columns],rsuffix='_matched')
                                )




        # expected_match = (match_e.apply(expected_mean).rename('expected_value_big_city').to_frame()
        #                 .join(car_demo_joined.Price_Sold_or_Highest_Bid_adj.rename('actual_value_small_city')))



        #save output
        # Temp.save_obj(expected_match,'expected_match_'+model.model_name)

        Temp.save_obj(expected_mean,'expected_match_'+model.model_name)


        # expected_match = Temp.load_obj('expected_match_'+model.model_name)

        #calculate difference between small and the large cities
        
        for ccol in compare_columns:
            expected_match_diff = expected_mean[[ccol,ccol+'_matched']].diff(axis=1).dropna(axis=1,how='all').dropna()
            print('\n-'+ccol,'\n' 
            ,'mean value: ',round(expected_mean[ccol].mean(),2),'\n'
            ,'mean value (matched): ',round(expected_mean[ccol+'_matched'].mean(),2),'\n'
            ,expected_match_diff.agg(['mean','std']).rename({ccol+'_matched':'difference'},axis=1) 
            ,'\nt-test p value:', round(stats.ttest_1samp(expected_match_diff.values.reshape(-1,),0)[1],3)#,'\n',expected_mean.head()
            )


        del car_demo_joined , expected_match_diff , expected_mean , match_null , match_e , match_l


if __name__ == "__main__":
    main()
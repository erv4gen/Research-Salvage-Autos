import pandas as pd
import numpy as np
import glob, sys, os, time , itertools , warnings , re , json
from bs4 import BeautifulSoup
from tqdm import tqdm
import matplotlib.pyplot as plt
import multiprocessing as mp
from scipy import stats
# from DataProcessing import 
import uszipcode , pickle 
from TempFolder.TempFolder import Temp


import plotly.figure_factory as ff
# importing all necessary libraries 
import chart_studio.plotly as py 
import plotly.graph_objs as go 
import pandas as pd 
  
# some more libraries to plot graph 
from plotly.offline import download_plotlyjs, init_notebook_mode, iplot, plot 
# To establish connection 
init_notebook_mode(connected = True) 


warnings.filterwarnings('ignore')

with open('nogit\\path','r') as f:
    path_to_csv = f.read()
path_to_processed_csv =path_to_csv + 'SC-csv\\'
Temp.set_path(path_to_csv +'SC-temp\\')
path_to_temp_csv = path_to_csv+'SC-working-folder\\'
colummn_names = ['Description'
                 ,'Title State/Type'
                 ,'Location'
                 ,'null'
                 ,'Auction Date'
                 ,'Actual Cash Value'
                 ,'Repair Cost'
                 ,'Odometer'
                 ,'Prim Damage'
                 ,'Sec Damage'
                 ,'Price Sold or Highest Bid']
path_to_makes = glob.glob(path_to_csv+'Cars\\*')
path_to_all_years = list(itertools.chain.from_iterable([glob.glob(path+'\\*') for path in path_to_makes]))


out_df = Temp.load_obj('out_df_working')

car_demo_joined = Temp.load_obj('car_demo_joined')


#find match candidates 
car_demo_joined.loc[car_demo_joined['CENSUS_closest']<1e6 , 'is_big_city'] = 0
car_demo_joined.loc[car_demo_joined['CENSUS_closest']>1e6 , 'is_big_city'] = 1

def find_match_expected(small_c, big_c):
    def find_match_for_row(x):
        # try:
        bs = big_c.copy()
        # import pdb; pdb.set_trace()
        bs['Actual_Cash_Value_adj'] = np.abs((x['Actual_Cash_Value_adj']/bs['Actual_Cash_Value_adj'])-1)
        bs['Odometer_Replace']  = np.abs((x['Odometer_Replace']/bs['Odometer_Replace'])-1)
        bs['Original_MSRP_mean']  = np.abs((x['Original_MSRP_mean']/bs['Original_MSRP_mean'])-1)
        bs = bs.fillna(9999)
        return bs.loc[
                        (bs.Actual_Cash_Value_adj  <=.1 )
                        & (bs.Odometer_Replace <=.1)
                        & (bs.Original_MSRP_mean <=.1)
                        ].index.tolist()

        # except: return []
    # import pdb; pdb.set_trace()
    return small_c.apply(find_match_for_row,axis=1)
    
match_l = []
i = 0 
for grp , df in tqdm(car_demo_joined.groupby(['Make','Model_short','Model_Age','Prim_Damage','Auction_Year'])):
    if len(df) <2:
        continue
    is_big_city_bs = df.loc[df.is_big_city == 1]
    is_small_city_df = df.loc[df.is_big_city == 0]

    match_l.append(find_match_expected(is_small_city_df,is_big_city_bs))
    i+=1

    # if i>550:
    #     break


match_null = pd.concat(match_l).dropna(axis=1).iloc[:,0].rename('match')

match_e = match_null.loc[match_null.map(lambda x: len(x)>0)]


Temp.save_obj(match_null,'expected_match_indexed')

print('% of records that matched: ', 100*len(match_e) / len(match_null),'%\n',match_e.head())

#expected match

def expected_mean(idx):
    return car_demo_joined.loc[idx,'Price_Sold_or_Highest_Bid_adj'].mean()

expected_match = (match_e.apply(expected_mean).rename('expected_value_big_city').to_frame()
.join(car_demo_joined.Price_Sold_or_Highest_Bid_adj.rename('actual_value_small_city'))) ; expected_match


Temp.save_obj(expected_match,'expected_match')



expected_match = Temp.load_obj('expected_match')
#calculate difference 
expected_match_diff = expected_match.diff(axis=1).dropna(axis=1).abs()

#calc statistics
print(expected_match_diff.agg(['mean','std']) ,'\nt-test p value:', round(stats.ttest_1samp(expected_match_diff.values.reshape(-1,),0)[1],3),'\n',expected_match.head())

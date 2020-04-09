# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'

#%%
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

#%%

# %%

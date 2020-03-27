# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
import pandas as pd
import numpy as np
import glob, sys, os, time , itertools , warnings , re , traceback
from bs4 import BeautifulSoup
from tqdm import tqdm
import matplotlib.pyplot as plt
import multiprocessing as mp
# from DataProcessing import 
import uszipcode , pickle 
from TempFolder.TempFolder import Temp

warnings.filterwarnings('ignore')


def properties_from_file(file_name):
    make , model , year  = file_name.split('\\')[-1].split('_')
    return make , model , year.split('.')[0]

with open('nogit/path', 'r') as f:
    datapath  = f.read()
path_to_root = datapath + 'SC-carfax\csv\*'
dirs = glob.glob(path_to_root)
data_list = []

# dir_file = dirs[0]
for dir_file in tqdm(dirs):
    make_files = glob.glob(dir_file+'\*')
    for file_path in make_files:
    # file_path = make_files[0]
        try:
            print('reading the file ',file_path)
            with open(file_path, 'r') as f:
                fle = f.read()
            if len(fle)>0:
                soup = BeautifulSoup(fle)
                print('extracting values')
                all_values = soup.findAll('li',{'class':'vehicle_details__item'})
                if len(all_values)>1:
                    values  = (all_values[0]
                                .text
                                .split('Original MSRP')[1]
                                .replace('$','')
                                .replace(',','')
                                .replace('N/A','')
                                .split(' - ')
                                )
                else:
                    values = [np.nan]
                make , model , year = properties_from_file(file_path)
                if len(values) < 2:
                    values.append(values[0])
                data_blob = dict(make = make, model = model,year = int(year) , Original_MSRP_low = float(values[0]), Original_MSRP_high = float(values[1]))
                data_list.append(data_blob)
            else:
                raise ValueError('Empty file')
        
        except Exception as e:
            print('error reading ',file_path,'\n',traceback.format_exc())
            # if str(e) != 'Empty file':

res_df = pd.DataFrame(data_list)
res_df.to_csv(datapath + "SC-carfax\processed\MSRP.csv")


print(res_df.to_string(),'\n\n',res_df.make.unique())
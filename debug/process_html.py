
import pandas as pd
import numpy as np
import glob, sys, os, time , itertools , warnings , re
from bs4 import BeautifulSoup
from tqdm import tqdm
import matplotlib.pyplot as plt
import multiprocessing as mp
# from DataProcessing import 
import uszipcode , pickle 
from TempFolder.TempFolder import Temp

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



#Read and parse scraped .html files 
def process_files(path_to_years__):
    res_df = pd.DataFrame()
    #for each make/year folder get a list of all .html files
    #for path_to__year in tqdm(path_to_years__):
    for path_to__year in path_to_years__:
        try:
            #list of all html
            path_to_make_years = glob.glob(path_to__year+'/*')

            #for each .html in the folder
            for file_path in path_to_make_years:
            #file_path = path_to_make_years[0]
            
                #read file and load to the BS object
                with open(file_path, 'r') as f:
                    fle = f.read()
                soup = BeautifulSoup(fle)

                rows = []
                line_ind = 0 
                
                #filter all div elements (div table)
                for div in soup.find_all('div',['row']):
                    row = div.text.strip()
                    #print(row)
                    try:
                        line_dict = {}
                        i = 0
                        
                        #clean each element and map to respected column
                        for line in row.replace('\t','').replace('\xa0','').split('\n'):
                            line_dict[colummn_names[i]]= line.replace('Location:','').replace('Title State/Type:','').strip()
                            i+=1
                        rows.append(line_dict)
                    except Exception as e:
                        #print(line_ind,'-',e)
                        pass
                    finally:
                        line_ind+=1

                page_df = pd.DataFrame(rows).iloc[3:,:-1]
                page_df_ext = page_df.join(page_df['Description'].str.split(' ',expand=True).rename(columns={0:'Year',1:'Make',2:'Model',3:'Model 2',4:'Model 3'}))
                page_df_ext = page_df_ext.join(page_df_ext['Location'].str.split('-',expand=True).rename(columns={0:'State',1:'City'}))
                page_df_ext['City'] = page_df_ext['City'].str.strip().str.capitalize()
                page_df_ext['State'] = page_df_ext['State'].str.strip()
                res_df = res_df.append(page_df_ext,ignore_index=True,sort=False)
                del rows , page_df_ext , page_df
        except:
            continue
    return res_df


if __name__ == '__main__':
    print('# files to concat' , len(path_to_all_years))
    NCPU = mp.cpu_count()
    print('Creating pool with ', NCPU,' CPUs')
    pool = mp.Pool(processes=4)
    # if not os.path.exists(path_to_processed_csv):
    #     print('Creating output folder')
    #     os.makedirs(path_to_processed_csv)
    
    by_chunk_path_to_all_years = np.array_split(path_to_all_years,len(path_to_all_years) // NCPU )
    start_ = 0
    end_ = len(by_chunk_path_to_all_years)
    for i in range(start_,end_):
        print('Starting with chunk: ',i,' of ',end_)
        path_to_all_years_split = np.array_split(by_chunk_path_to_all_years[i],NCPU)
        ress = pool.map(process_files,path_to_all_years_split)
        ress_df = pd.concat(ress)
        print('Chunk ',i,' is done.')
        ress_df.to_csv(path_to_processed_csv+'sc_'+str(i)+'.csv',index=False)
#         i+=1
#         if i ==2:
#             break
    pool.close()
    pool.join()
    print('all work is done')
    

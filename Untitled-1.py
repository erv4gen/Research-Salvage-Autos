# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
from IPython import get_ipython

# %%
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


# %%
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
search = uszipcode.SearchEngine(simple_zipcode=True)
path_to_all_years = list(itertools.chain.from_iterable([glob.glob(path+'\\*') for path in path_to_makes]))
search = uszipcode.SearchEngine(simple_zipcode=True)


# %%
# #%%time
# '''
# Parse the HTML files
# '''

# #
# #Read and parse scraped .html files 
# def process_files(path_to_years__):
#     res_df = pd.DataFrame()
#     #for each make/year folder get a list of all .html files
#     #for path_to__year in tqdm(path_to_years__):
#     for path_to__year in path_to_years__:
#         try:
#             #list of all html
#             path_to_make_years = glob.glob(path_to__year+'/*')

#             #for each .html in the folder
#             for file_path in path_to_make_years:
#             #file_path = path_to_make_years[0]
            
#                 #read file and load to the BS object
#                 with open(file_path, 'r') as f:
#                     fle = f.read()
#                 soup = BeautifulSoup(fle)

#                 rows = []
#                 line_ind = 0 
                
#                 #filter all div elements (div table)
#                 for div in soup.find_all('div',['row']):
#                     row = div.text.strip()
#                     #print(row)
#                     try:
#                         line_dict = {}
#                         i = 0
                        
#                         #clean each element and map to respected column
#                         for line in row.replace('\t','').replace('\xa0','').split('\n'):
#                             line_dict[colummn_names[i]]= line.replace('Location:','').replace('Title State/Type:','').strip()
#                             i+=1
#                         rows.append(line_dict)
#                     except Exception as e:
#                         #print(line_ind,'-',e)
#                         pass
#                     finally:
#                         line_ind+=1

#                 page_df = pd.DataFrame(rows).iloc[3:,:-1]
#                 page_df_ext = page_df.join(page_df['Description'].str.split(' ',expand=True).rename(columns={0:'Year',1:'Make',2:'Model',3:'Model 2',4:'Model 3'}))
#                 page_df_ext = page_df_ext.join(page_df_ext['Location'].str.split('-',expand=True).rename(columns={0:'State',1:'City'}))
#                 page_df_ext['City'] = page_df_ext['City'].str.strip().str.capitalize()
#                 page_df_ext['State'] = page_df_ext['State'].str.strip()
#                 res_df = res_df.append(page_df_ext,ignore_index=True,sort=False)
#                 del rows , page_df_ext , page_df
#         except:
#             continue
#     return res_df


# if __name__ == '__main__':
#     print('# files to concat' , len(path_to_all_years))
#     NCPU = mp.cpu_count()
#     print('Creating pool with ', NCPU,' CPUs')
#     pool = mp.Pool(processes=4)
#     # if not os.path.exists(path_to_processed_csv):
#     #     print('Creating output folder')
#     #     os.makedirs(path_to_processed_csv)
    
#     by_chunk_path_to_all_years = np.array_split(path_to_all_years,len(path_to_all_years) // NCPU )
#     start_ = 0
#     end_ = len(by_chunk_path_to_all_years)
#     for i in range(start_,end_):
#         print('Starting with chunk: ',i,' of ',end_)
#         path_to_all_years_split = np.array_split(by_chunk_path_to_all_years[i],NCPU)
#         ress = pool.map(process_files,path_to_all_years_split)
#         ress_df = pd.concat(ress)
#         print('Chunk ',i,' is done.')
#         ress_df.to_csv(path_to_processed_csv+'sc_'+str(i)+'.csv',index=False)
# #         i+=1
# #         if i ==2:
# #             break
#     pool.close()
#     pool.join()
#     print('all work is done')
    


# %%
#Read parsed files
outputs = glob.glob(path_to_processed_csv+'*')
out_l = []
errs = []
for output in tqdm(outputs):
    try:
#     print(output)
        out_l.append(pd.read_csv(output))
    except:
        errs.append(output)
out_df = pd.concat(out_l,ignore_index=True,sort=False)

if len(errs) >0:
    print('Errors:', errs)


# %%
print(out_df.columns)

row_size = out_df.shape[0]

print('Raw dataset size: ',row_size)


out_df.head(50)


# %%


# out_df = out_df.join(out_df['Location'].str.split('-',expand=True).rename(columns={0:'State',1:'City'}))

out_df['City'] = out_df['City'].str.strip().str.capitalize()
out_df['State'] = out_df['State'].str.strip().str.upper()
out_df['Model'] = out_df['Model'].astype('str') + ' '+out_df['Model 2'].astype('str').fillna('') + ' ' + out_df['Model 3'].fillna('').astype('str')

out_df.head(20)


# %%
out_df.groupby('Make')['Year'].count().sort_values(ascending=False)[:60]


# %%
out_df['Make'] = out_df['Make'].str.strip().str.upper()

out_df.groupby('Make')['Year'].count().sort_values(ascending=False)[:60]


# %%
#clean up wrong values 
out_df = out_df.loc[~out_df['Actual Cash Value'].str.contains('Odometer').fillna(False)]
out_df = out_df.loc[~out_df['Year'].str.contains('Make').fillna(False)]
out_df = out_df.loc[~out_df['Year'].str.contains('Model:').fillna(False)]
out_df = out_df.loc[~out_df['Auction Date'].str.contains('Repair Cost').fillna(False)]


#filter out canadian cars
out_df = out_df.loc[~out_df['Price Sold or Highest Bid'].str.contains('CA').fillna(False)
                               ]

out_df['Auction Date'] = pd.to_datetime(out_df['Auction Date'])

out_df['Auction_Year'] = out_df['Auction Date'].dt.year


# %%
cleaned_size = out_df.shape[0]
print('Cleaned dataset size:',cleaned_size
      ,'\nRows removed: ', row_size - cleaned_size)


# %%
# # out_df.to_csv(path_to_processed_csv+'SC-countymap.csv',index=False)

# # out_df = pd.read_csv(path_to_processed_csv+'SC-countymap.csv')

# from DataProcessing.DataStats import get_df_stats

# get_df_stats(out_df)


# %%
# Combining Prim Damages 
raw_prim_cat = out_df['Prim Damage'].str.lower().unique()
print('Cat count:',raw_prim_cat.shape[0],'\n',raw_prim_cat)


# %%
prim_damage_map = {
 'burn - engine': 'burn'
, 'partial/incomplete r' : 'partial repair'
, 'frame damage reporte' : 'frame damage'

,'unknown' : 'no data'


, np.nan : 'no data'
,'price sold or highest bid' :'no data'
,'rr' : 'no data'

}
out_df['Prim Damage'] = out_df['Prim Damage'].str.lower().replace(prim_damage_map)


# %%
new_prim_cat = out_df['Prim Damage'].str.lower().unique()
print('Cat count:',new_prim_cat.shape[0],'\n',new_prim_cat)


# %%
Temp.save_obj(out_df,'out_df' )#140520646600464


# %%
out_df = Temp.load_obj('out_df')


# %%
#fix numerical values
def alter_sting(x):    
    try:
        return float(re.sub('[\$,USD,\, ,E,A,N]','',x))
    except:
#         print(x)
        return np.nan


out_df['Actual Cash Value'] = (out_df['Actual Cash Value']
                               #.fillna(-1)
                               .map(alter_sting))
out_df['Repair Cost'] = (out_df['Repair Cost']
#                          .fillna(-1)
                         .map(alter_sting))

out_df['Price Sold or Highest Bid'] = (out_df['Price Sold or Highest Bid']
                                       #.fillna(-1)
                                       .map(alter_sting))
out_df['Year'] = (out_df['Year']
#                   .fillna(-1)
                  .map(float))

out_df = out_df.rename(columns={'Year':'Model_Year'
                               }
                      ).assign(Model_Year = lambda x: pd.to_datetime(x['Model_Year'].astype(int).astype(str)))



#rename variables - add _ where needed

out_df = out_df.rename({'Price Sold or Highest Bid':'Price_Sold_or_Highest_Bid'
                            ,'Repair Cost':'Repair_Cost'
                            ,'Actual Cash Value':'Actual_Cash_Value'
                            ,'Prim Damage':'Prim_Damage'
                            ,'Sec Damage':'Sec_Damage'
                            ,'Title State/Type':'Title_State_Type'
                            ,'Auction Date':'Auction_Date'
                            },axis=1)


print('New columns names:',out_df.columns)


# %%
#explore outlyers for 
fig, ax = plt.subplots(1,2,figsize=(7,5))
out_df['Actual_Cash_Value'].plot.box(ax=ax[0])
out_df['Actual_Cash_Value'].plot.box(ax=ax[1],showfliers =False)


# %%

#display modes of the distribution 
filterd_acv = out_df['Actual_Cash_Value'].loc[out_df['Actual_Cash_Value'] <1e4]

print('Counts:\n#>100k:'
                ,len(out_df['Actual_Cash_Value'].loc[out_df['Actual_Cash_Value']>=1e5])
            
)

n = 10
filterd_acv_round = (filterd_acv // n ) * n


#plot a histogram of observed cash_values
fig= plt.figure()
ax = filterd_acv.plot.hist()
ax.set(title = 'Histogram for Actual_Cash_Value < 10k');



#print modes 
filterd_acv_round.value_counts().iloc[:10]


# %%
#fit  normat distribution

from scipy import stats

x = np.linspace(0,filterd_acv.max(),len(filterd_acv))
mu , std = stats.norm.fit(filterd_acv.values)

ax = filterd_acv.plot.hist(normed=True)
plt.plot(x, stats.norm.pdf(x , mu, std))
ax.set(title = 'Histogram for Actual_Cash_Value < 10k');
print('K-S p-value:', stats.kstest(filterd_acv.values , 'norm')[1])


# %%
#format odometer data

#for null model - replace all E  & N values into "No Data" (np.nan)

def odometer_null(x):
    if 'E' in x or 'N' in x:
        return np.nan
    else:
        return alter_sting(x)


    
#create a dictionary for max E value for each model 

odometr_dict = (out_df.loc[out_df['Odometer'].str.contains('A')]
                 .assign(odometer_num = lambda x: x['Odometer'].map(alter_sting)
                        )
                 .groupby('Description')['odometer_num']
                 .max()
                #  [['Odometer','odometer_num']]
                )

#replace E and N values into max of A category
def odometer_replace(x):
    if 'E' in x['Odometer'] or 'N' in x['Odometer']:
        try:
            return odometr_dict[x['Description']]
        except:
            return np.nan
    else:
        return alter_sting(x['Odometer'])


#create null model
out_df = out_df.assign(Odometer_Null = lambda x: x['Odometer'].map(odometer_null))

#create replace model:

out_df = out_df.assign(Odometer_Replace = lambda x: x.apply(odometer_replace,axis=1))


# create Model_Age
# current_year = 2019
out_df['Model_Age'] = (out_df['Auction_Date'] - out_df['Model_Year'] ).dt.days


# %%

numerical_fields =  ['Actual_Cash_Value'
               # 'Auction_Date',
       #'Odometer'
       , 'Price_Sold_or_Highest_Bid', 'Repair_Cost',
        'Odometer_Null','Odometer_Replace'
        ,'Model_Age'
        ]

categorical_variables = ['Prim_Damage','Make','Model'
]


# %%
#categories for all categorical variables 

# categorial_variables = ['Prim_Damage'
#                         ,'Sec_Damage'
#                         ,'Make'
#                         ,'Model' # - to many categories >2000
#
                        #  ]  
out_df[categorical_variables]  =out_df[categorical_variables].astype('category')

print(out_df[categorical_variables].describe().to_string(),'\n\n')

cat_df = {}
for col in categorical_variables:
    print(col, ' - ', out_df[col].cat.categories.tolist(),'\n')
    cat_df[col] = out_df[col].cat.categories.tolist()


# %%
# out_df = Temp.load_obj('140505604672312')
# Temp.save_obj(out_df,'out_df')


# %%
out_df[numerical_fields].describe(percentiles =[.01,.25, .5, .75,.99])


# %%
#Correlation Analysis
out_df[numerical_fields].corr()


# %%
var


# %%
out_df[var]


# %%
#histomgram for conitnuous variables
fig , axs = plt.subplots(len(numerical_fields) , figsize=(8,22))
i=0
for var in numerical_fields:
    out_df[var].plot.hist(bins=10, ax= axs[i])
    axs[i].set(title = var)
    i+=1


# %%
out_df[categorical_variables]

from DataProcessing.DataStats import get_df_stats

get_df_stats(out_df[categorical_variables])


# %%
out_df.columns


# %%
#value distribution for Description 

desc_val_dist = out_df['Description'].value_counts()

desc_val_dist[:30]


# %%
#value distribution for Prim Damage 

prim_val_dist = out_df['Prim_Damage'].value_counts()

prim_val_dist[:30]


# %%
#summary statistics conditions on Prim_Damage

cond_on_primd = out_df.groupby('Prim_Damage').agg(['mean','var']).stack().sort_index(level=1)
cond_on_primd.index = ['{} - {}'.format(j, i) for i, j in cond_on_primd.index]
cond_on_primd


# %%
#summary statistics conditions on Make

cond_on_make = out_df.groupby('Make').agg(['mean','var']).stack().sort_index(level=1)
cond_on_make.index = ['{} - {}'.format(j, i) for i, j in cond_on_make.index]
cond_on_make.to_csv('csv/temp.csv')


# %%
with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
    print(cond_on_make)


# %%
get_ipython().run_cell_magic('time', '', "# i = 6\n# city = out_df.loc[i,'City']\n# state =out_df.loc[i,'State']\n# city, state\n\n# geo_found = search.by_city_and_state(city,state)[0]\n\n# geo_found.county , geo_found.state\n\n\ndef find_county(x):\n    try:\n        return search.by_city_and_state(x[1] , x[0])[0].county\n    except:\n        return 'None'\n\n\nlocs = out_df['Location'].unique()\nlocs = locs[~pd.isnull(locs)]\n\nstate_city = [l[:2] for l in  list(map(lambda x: x.split(' - '),   locs ) ) #if len(l)>1\n             ]\n\n\n\n\n\ncounty = list(map(find_county,   state_city ) )\n\ncounty = [', '.join([l.strip() for l in (county[i].replace('County','')+', '+state_city[i][0]).split(',') ])\n for i in range(len(county))]\n\nloc_lookup = pd.DataFrame({'Location':locs,'County':county}).set_index('Location')['County']\n\nloc_lookup\n\n\ndef map_countries(x):\n    try:\n        return loc_lookup.loc[x]\n    except:\n        return 'None'\n        \nout_df['County'] = out_df['Location'].map(map_countries)")


# %%
out_df.head(10)


# %%
'''
JOINING WITH DEMOGRAPHIC DATA 
'''


# %%
#read the demography data `

path_to_demograpyh ='c:\\data\\Datasets\\SC-working-folder\\demography.csv'

demog = pd.read_csv(path_to_demograpyh)


# %%
print('Demographic features:\n' , demog.columns.tolist())


# %%
final_columns = [
'Actual_Cash_Value'
, 'Auction_Date'
,'Price_Sold_or_Highest_Bid'
,'Prim_Damage'
,'Repair_Cost'
,'Sec_Damage'
,'Model_Year'
,'Make'
,'Model'
,'Auction_Year'
,'County'
,'Odometer_Null'
,'Odometer_Replace'
,'Model_Age'
,'Unemp-Pct-2014'
,'Unemp-Pct-2015'
,'Unemp-Pct-2016'
,'Unemp-Pct-2017'
,'Unemp-Pct-2018'
,'Unemp-Pct-2013'
,'Unemp-Pct-2012'
,'Unemp-Pct-2011'
,'Unemp-Pct-2010'
,'Income_adj-1990'
,'Income_adj-1991'
,'Income_adj-1992'
,'Income_adj-1993'
,'Income_adj-1994'
,'Income_adj-1995'
,'Income_adj-1996'
,'Income_adj-1997'
,'Income_adj-1998'
,'Income_adj-1999'
,'Income_adj-2000'
,'Income_adj-2001'
,'Income_adj-2002'
,'Income_adj-2003'
,'Income_adj-2004'
,'Income_adj-2005'
,'Income_adj-2006'
,'Income_adj-2007'
,'Income_adj-2008'
,'Income_adj-2009'
,'Income_adj-2010'
,'Income_adj-2011'
,'Income_adj-2012'
,'Income_adj-2013'
,'Income_adj-2014'
,'Income_adj-2015'
,'Income_adj-2016'
,'Income_adj-2017'
,'CENSUS2010POP'
,'ESTIMATESBASE2010'
,'POPESTIMATE2010'
,'POPESTIMATE2011'
,'POPESTIMATE2012'
,'POPESTIMATE2013'
,'POPESTIMATE2014'
,'POPESTIMATE2015'
,'POPESTIMATE2016'
,'POPESTIMATE2017'
,'POPESTIMATE2018'
,'GRNDTOT'
,'CPOPARST'
       ]

car_demo_joined = (pd.merge(out_df, demog,left_on='County',right_on='index',how='left')
                    [final_columns]
                    )


# %%
car_demo_joined.head()


# %%
car_demo_joined['Auction_Date'] = pd.to_datetime(car_demo_joined['Auction_Date'])


# %%
#market size

market_size = car_demo_joined.groupby(car_demo_joined['Auction_Date'].dt.year)['Actual_Cash_Value'].sum()

market_size =  market_size / 1000000

print(market_size.to_string())
plt.figure(figsize=(10,4))
ax = market_size.plot.bar()
ax.set(title='Market Size', ylabel='$, millions');


# %%
car_demo_joined.head()


# %%
car_demo_joined.to_csv(path_to_temp_csv+'joint_working_df.csv')


# %%
by_couny = (car_demo_joined.reset_index()
             .groupby('County')['index']
             .count().sort_values(ascending=False)
           )

print(by_couny.to_string())


# %%
plt.figure()
ax = by_couny[:20].plot.bar()
ax.set(title='Top 20 Counties by # records',ylabel='# observations');


# %%
#categories for all categorical variables 
# categorial_variables = ['Prim_Damage'
#                         ,'Sec_Damage'
#                         ,'Make'
#                         ,'Model' # - to many categories >2000
#
                        #  ]  
out_df[categorial_variables]  =out_df[categorial_variables].astype('category')

print(out_df[categorial_variables].describe().to_string(),'\n\n')

cat_df = {}
for col in categorial_variables:
    print(col, ' - ', out_df[col].cat.categories.tolist(),'\n')
    cat_df[col] = out_df[col].cat.categories.tolist()


# %%



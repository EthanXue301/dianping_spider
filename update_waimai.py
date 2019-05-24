# -*- coding: utf-8 -*-
import requests,re,json,pickle,os
from lxml import etree
from pymongo import MongoClient
from bs4 import BeautifulSoup
from CrawlFunctions import getSoup,getEtreeHtml,getSoup
from multiprocessing.dummy import Lock,Pool
import numpy as np
import pandas as pd
from datetime import datetime

    
client=MongoClient()
db=client.foodstore
clct = client.foodstore.bjstore
shop_result_file = '/Users/xuegeng/Downloads/result-0.json'

with open(shop_result_file,'r') as f:
    origin_is_shop_waimai = json.load(f)
        

#for k,v in origin_is_shop_waimai.iteritems():
#    if not v:
#        print k

def to_df():
    global clct,mongo_lock
    cur = clct.find({})
    return pd.DataFrame(list(cur))

df = to_df()
df.shop_name = df.shop_name.apply(lambda x:x.replace(u"添加分店","").replace("\n",""))
del df['_id']

df = df.drop_duplicates('shop_id')

#df.to_excel('/Users/xuegeng/Desktop/toxzm.xlsx')


df2 = pd.DataFrame(origin_is_shop_waimai.items(),columns=['shop_id','has_waimai'])
df2.shop_id = df2.shop_id.apply(lambda x:x.split('/')[-1])
df3 = pd.merge(df,df2,on='shop_id')


region_list = np.unique(df3.region)
do = dict()
for i,v in enumerate(region_list):
    do[v]=i
df3.region = df3.region.apply(lambda x:do[x])  

catagory_list = np.unique(df3.catagories)
do = dict()
for i,v in enumerate(catagory_list):
    do[v]=i
df3.catagories = df3.catagories.apply(lambda x:do[x])  


has_waimai_list = np.unique(df3.has_waimai)
do = dict()
for i,v in enumerate(has_waimai_list):
    do[v]=i
df3.has_waimai = df3.has_waimai.apply(lambda x:do[x])  

df3 = df3[df3.total_rank!='']
df3 = df3[df3.total_rank!='mid-str']
df3.total_rank = df3.total_rank.apply(lambda x:float(re.findall('\d+',x)[0])/10)


def lambda_for_has_comm(x):
    if x>0:
        return 1
    else:
        return 0

df3['has_comm']=df3['com_count'].apply(lambda_for_has_comm)

d = dict(zip(df3.shop_id,df3.com_per))#r人均改为左边
#df6['com_per'] = df6['shop_id'].map(d)


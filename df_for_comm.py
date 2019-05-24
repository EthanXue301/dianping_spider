# -*- coding: utf-8 -*-
import re,json,os,itertools
from pymongo import MongoClient
from CrawlFunctions import getSoup
from parseItems import parse_item
import numpy as np
import pandas as pd
from datetime import datetime
from collections import Counter
from multiprocessing.dummy import Lock,Pool

dir_path = '/Users/xuegeng/Spider_Workspace/crawler/shop_comment_parsed_xg2'
format_json_filename = dir_path+'/%s.json'


def return_resolved_ids():
    from os import listdir
    from os.path import isfile, join
    onlyfiles = [f for f in listdir(dir_path) if isfile(join(dir_path, f))]
    try:onlyfiles.remove('.DS_Store')
    except ValueError:pass
    l2 = map(lambda x:x.split('.')[0],onlyfiles)
    return list(np.unique(l2))


def lambda_for_parse(id_):
    with open(format_json_filename%id_,'r') as f:
        js  = json.load(f)
    
    return js

laa = []
for ii in return_resolved_ids():
    try:
        laa.append(lambda_for_parse(ii))
    except ValueError:
        print ii
df5 = pd.DataFrame(laa)


#df6 = pd.merge(df3,df5,on='shop_id',how='right')
#df6 = df6.dropna()
#df6.to_excel('/Users/xuegeng/Desktop/toxzm.xlsx')


#df.avg_bus_rpl_comm = df.avg_bus_rpl_comm-1
#df.avg_bus_rpl_comm2 = df.avg_bus_rpl_comm2-1
#df.avg_bus_rpl_comm3 = df.avg_bus_rpl_comm3-1



# -*- coding: utf-8 -*-
import requests,re,json,pickle,os
from lxml import etree
from pymongo import MongoClient
from bs4 import BeautifulSoup
from CONSTANTS import HEADER2
from CrawlFunctions import getSoup,getEtreeHtml,getSoup
from multiprocessing.dummy import Lock,Pool
import numpy as np
from datetime import datetime

    
client=MongoClient()
db=client.foodstore

file_name  = '/Users/xuegeng/Spider_Workspace/crawler/shop_htmls/shop_5642142.html'
lock = Lock()


def parse_file(file_name):
    global lock
    
    with open(file_name,'r') as f:
        text = f.read()
    soup = getSoup(text)
    shop_id = file_name.split('_')[-1].split('.')[-2]
        
        
    d = parse_soup(soup)
    d['shop_id'] = shop_id
    
    with lock:
        db.bjstore.insert_one(d)

    
def parse_soup(soup):
    d = {}
    
    #总星级
    try:total_rank = soup.find(class_="brief-info").find('span')['class'][1]
    except :total_rank = ''
    d['total_rank'] = total_rank
    
    #评论数
    try:com_count = int(re.findall('\d+',soup.find(class_="brief-info").find(id="reviewCount").text)[0])
    except :com_count = np.nan
    d['com_count'] = com_count
    
     #人均
    try:com_per = int(re.findall('\d+',soup.find(id="avgPriceTitle").text)[0])
    except :com_per = np.nan
    d['com_per'] = com_per
    
     #口味
    try:taste = float(re.findall('\d\.\d',soup.find(id="comment_score").find_all('span')[0].text)[0])
    except :taste = np.nan
    d['taste'] = taste
    
     #环境
    try:env = float(re.findall('\d\.\d',soup.find(id="comment_score").find_all('span')[1].text)[0])
    except :env = np.nan
    d['env'] = env
    
     #服务
    try:serv = float(re.findall('\d\.\d',soup.find(id="comment_score").find_all('span')[2].text)[0])
    except :serv = np.nan
    d['serv'] = serv
    
    #名字
    try:shop_name = soup.find(class_="shop-name").text.split(' ')[0]
    except :shop_name = ''
    d['shop_name'] = shop_name
    
    
    #分店数量
    try:branch_num = int(re.findall('\d+',soup.find(class_="branch J-branch").text)[0])
    except :branch_num = 0
    d['branch_num'] = branch_num
    
    #所属类型
    try:catagories = soup.find(class_="breadcrumb").find_all('a')[2].text.replace(' ','').replace('\n','')
    except :catagories = ''
    d['catagories'] = catagories
    
    #所在商区
    try:region = soup.find(class_="breadcrumb").find_all('a')[1].text.replace(' ','').replace('\n','')
    except :region = ''
    d['region'] = region
    
    return d

#count = 0
#
#for root, dirs, files in os.walk("/Users/xuegeng/Spider_Workspace/crawler/shop_htmls/", topdown=False):
#    for name in files:
#        fp = os.path.join(root, name)
#        try:
#            parse_file(fp)
#            print "[+] Processing "+fp+" Complete: " + format(count/990.0,".5f") + "%"
#            count += 1
#        except AttributeError:
#            print "[-] It might be a lousy store: "+fp
        

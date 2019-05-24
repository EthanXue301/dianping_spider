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

#full_path = os.path.realpath(__file__)
#path, filename = os.path.split(full_path)
#print(path + ' --> ' + filename + "\n")

file_name  = '/Users/xuegeng/Spider_Workspace/crawler/htmls/shop_2703391/shop_2703391_p7.html'
lock = Lock()


def lambda_item(item):
        try:
            d = parse_item(item)
            return d
        except:
            print "[-] There might be a lousy comment in "+file_name
            return {}
        

def parse_file(file_name):
    global lock
    
    with open(file_name,'r') as f:
        text = f.read()
    soup = getSoup(text)
    shop_id = file_name.split('_')[-2]
    try:
        items = soup.find(class_="comment-list").find_all(id = re.compile("rev_"))
    except AttributeError:
        return
    
    def lambda_add_shop_id(d):
        d['shop_id'] = shop_id
        return d
    
    all_comment_dict = map(lambda_item,items)
    all_comment_dict = map(lambda_add_shop_id,all_comment_dict)
    with lock:
        try:
            db.bjfoodstore.insert_many(all_comment_dict)
        except TypeError:
            pass
    
def parse_item(item):
    d = {}
    try:
        total_rank =  item.find(class_=re.compile("item-rank-rst"))['class'][-1]
        total_rank = int(total_rank.strip('irr-star'))/10
    except TypeError:total_rank =np.nan
    d['total_rank'] = total_rank
    
    sub_rank = item.find(class_="content").find_all(class_="rst")
    try:taste =  int(sub_rank[0].text.strip(u'\u53e3\u5473').split('(')[0])
    except IndexError:taste =np.nan
    d['taste'] = taste
    
    try:env = int(sub_rank[1].text.strip(u'\u73af\u5883').split('(')[0])
    except IndexError:env =np.nan
    d['env'] = env
    
    try:serv = int(sub_rank[2].text.strip(u'\u670d\u52a1').split('(')[0])
    except IndexError:serv =np.nan
    d['serv'] = serv
    
    #商家回复时间，无为None
    try:
        bus_rep_time_text = item.find(class_ = re.compile("J-busi-reply")).find(class_ = "time").text
        try:
            bus_rep_time = '20' + bus_rep_time_text
            bus_rep_time = datetime.strptime(bus_rep_time,'%Y-%m-%d %H:%M:%S')
        except ValueError:
            bus_rep_time = '2017-' + bus_rep_time_text
            bus_rep_time = datetime.strptime(bus_rep_time,'%Y-%m-%d %H:%M:%S')
    except AttributeError,e:
        bus_rep_time = None
    d['bus_rep_time'] = bus_rep_time
    
    #评论时间，无为None
    comm_time_text = item.find(class_ = "time").text.split(u'\xa0\xa0')[0]
    try:
        comm_time = '20' + comm_time_text
        comm_time = datetime.strptime(comm_time,'%Y-%m-%d')
    except ValueError:
        comm_time = '2017-' + comm_time_text
        comm_time = datetime.strptime(comm_time,'%Y-%m-%d')
    d['comm_time'] = comm_time
    
    #消费人均，无为nan
    try:com_per = int(item.find(class_='comm-per').text.split(u'\uffe5')[-1])
    except AttributeError:com_per =np.nan
    d['com_per'] = com_per
    
    #评论用户ID
    try:com_id = item.find(class_='name').text
    except AttributeError:com_id =''
    d['com_id'] = com_id
    
    #是否带图
    try:has_picture = 'shop-photo' in str(item)
    except AttributeError,TypeError:has_picture  = False
    d['has_picture'] = has_picture
    
    #内容 内容长度
    try:
        content = item.find(class_ = 'J_brief-cont').text
        content_len = len(content)
    except AttributeError,TypeError:
        content = ''
        content_len = 0
    d['content'] = content
    d['content_len'] = content_len
    
    #回复内容 内容长度
    try:
        reply_content = item.find(class_ = re.compile("J-busi-reply")).text.split(']')[1].split('\n')[0]
        reply_content_len = len(reply_content)
    except AttributeError,TypeError:
        reply_content = ''
        reply_content_len = 0
    d['reply_content'] = reply_content
    d['reply_content_len'] = reply_content_len
    
    #点赞数
    try:heart_num = eval(item.find(class_="heart-num").text)
    except AttributeError,TypeError:heart_num = 0
    d['heart_num'] = heart_num
    
    #回应数
    try:recomment_num = eval(item.find(class_="J_rtl").text)
    except AttributeError,TypeError:recomment_num = 0
    d['recomment_num'] = recomment_num
    
    return d




with open(file_name,'r') as f:
        text = f.read()
soup = getSoup(text)
shop_id = file_name.split('_')[-2]
items = soup.find(class_="comment-list").find_all(id = re.compile("rev_"))
d = parse_item(items[0])






#count = 0
#
#for root, dirs, files in os.walk("/Users/xuegeng/Spider_Workspace/crawler/htmls/", topdown=False):
#    for name in files:
#        fp = os.path.join(root, name)
#        try:
#            parse_file(fp)
#            print "[+] Processing "+fp+" Complete: " + format(count/7980.0,".5f") + "%"
#            count += 1
#        except AttributeError:
#            print "[-] It might be a lousy store: "+fp
        
        
#ul = []
#st = set()
#for num in range(1,12):
#    ff = '/Users/xuegeng/Spider_Workspace/crawler/htmls/shop_2703391/shop_2703391_p%d.html'%num
#    u = parse_file(ff)
#    #ul.extend(u)
#    for iu in u:
#        d = parse_item(iu)
#        #if d['total_rank']==u'\u661f\u7ea7':
#        #    print ff
#            
#        print d['bus_rep_time']
    
#for i in st:print i#{u'\u597d', u'\u5f88\u597d', u'\u975e\u5e38\u597d'}
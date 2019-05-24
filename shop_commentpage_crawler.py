# -*- coding: utf-8 -*-
import requests,re,json,pickle,os
from lxml import etree
from bs4 import BeautifulSoup
from CONSTANTS import HEADER2
from CrawlFunctions import getSoup,getEtreeHtml,getSoup
from multiprocessing.dummy import Lock,Pool

lock = Lock()

with open('/Users/xuegeng/Spider_Workspace/crawler/is_shop_crawled3.json','r') as f:
        origin_is_page_crawled = json.load(f)

pages_to_crawl = origin_is_page_crawled.copy()

for it in origin_is_page_crawled.iteritems():
    if it[1]:
        del pages_to_crawl[it[0]]


def describe():
    global origin_is_page_crawled
    try:
        count = 0
        for i in origin_is_page_crawled.iterkeys():
            if origin_is_page_crawled[i]:
                count += 1
        print "[=] %d pages crawled"%count
        print "[=] %d pages to crawl"%(len(origin_is_page_crawled)-count)
    except:
        pass
describe()

def getFilePath(url):
    
    url1 =  url.split('/')            
    dir_name = url1[-2]+'_'+url1[-1]
    dir_path = '/Users/xuegeng/Spider_Workspace/crawler/shop_htmls'  +os.sep+dir_name+'.html'
    return dir_path
    
def getHtmlText(url):
    '''return text-form html'''
    try:
        html = requests.get(url, headers = HEADER2, timeout = 5)
        html.encoding = 'utf-8'
        return html.text
    except:
        print "[-] Fail to get " + url
        return None        


def isBanned(text):
    
    """make sure if a thread is caught as spider, if so , change cookie"""
    
    chptcha_url = 'http://www.dianping.com/alpaca/captcha.jpg'
    if chptcha_url in text:
        return True
    return False
    

def writeHtml(file_name,text):
    '''将html文件写入本地'''
    with open(file_name,'w') as f:
        f.write(text.encode('utf-8'))

          
def saveHtml(html_text,url):
    
    global origin_is_page_crawled
    
    #从url提取出文件路径名
    file_path = getFilePath(url)
    #如果text里面有验证码：return
    if isBanned(html_text):
        return
        
    #如果没有验证码且不为空，则存储，修改origin_is_page_crawled的布尔
    try:
        writeHtml(file_path,html_text)
        origin_is_page_crawled[url] = True
        print "[+] Saving %s succeed!"%file_path
    except:
        origin_is_page_crawled[url] = False
        print "[-] Saving %s Failed!"%file_path
        
       
def mergeIntoTheResult(page_url):
    global origin_is_page_crawled,lock
    file_path = getFilePath(page_url)
    
    if os.path.exists(file_path):
        origin_is_page_crawled[page_url] = True
        return 
        
    page_text = getHtmlText(page_url)
    if page_text:
        with lock:
            saveHtml(page_text,page_url)
            
def processMerging():
    '''to have process handle shop url crawling'''
    global pages_to_crawl,origin_is_page_crawled,shop_urls
    pool = Pool(processes = 25)
    results = pool.map(mergeIntoTheResult, pages_to_crawl.keys())
    pool.close()
    pool.join()
    with open('/Users/xuegeng/Spider_Workspace/crawler/is_shop_crawled3.json','w') as f:
        json.dump(origin_is_page_crawled,f)



processMerging()
describe()
with open('/Users/xuegeng/Spider_Workspace/crawler/is_shop_crawled3.json','w') as f:
        json.dump(origin_is_page_crawled,f)


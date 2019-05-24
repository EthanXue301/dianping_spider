# -*- coding: utf-8 -*-
import requests,re,json,pickle,os
from lxml import etree
from bs4 import BeautifulSoup
from CONSTANTS import HEADER2
from CrawlFunctions import getSoup,getEtreeHtml,getSoup
from shop_commentpage_crawler import generatePageList,getMaxPageNum,getHtmlText
from multiprocessing.dummy import Lock,Pool

lock = Lock()

with open('/Users/xuegeng/Desktop/crawler/shop_comment_pages_crawled.json','r') as f:
        origin_is_page_crawled = json.load(f)


def change_page_crawled(fpath):
    global origin_is_page_crawled
    l = fpath.split('/')[-1].split('.')[0].split('_')
    num = l[-1][-1]
    sid = l[1]
    url = 'https://www.dianping.com/shop/'+sid+'/review_more?pageno='+num
    origin_is_page_crawled[url] = True
    with open(fpath,'r') as f:
        html_text = f.read()
    soup = getSoup(html_text)
    max_page_num = getMaxPageNum(soup)
    if max_page_num > 1:
        l = generatePageList(url,max_page_num)
        print "[+] Appending %d pages"%len(l)
        for u in l:
            origin_is_page_crawled[u] = False

for root, dirs, files in os.walk("/Users/xuegeng/Desktop/crawler/htmls", topdown=False):
    for name in files:
        fp = os.path.join(root, name)
        try:
            change_page_crawled(fp)
            print "[+] Working on " + fp
        except:
            print "[-] Wrong with " + fp

with open('/Users/xuegeng/Desktop/crawler/shop_comment_pages_crawled_updated.json','w') as f:
        json.dump(origin_is_page_crawled,f)
# -*- coding: utf-8 -*-
import requests,re,json,pickle
from lxml import etree
from bs4 import BeautifulSoup
from CONSTANTS import HEADER2
from CrawlFunctions import getSoup,getEtreeHtml,getSoup
from multiprocessing.dummy import Lock,Pool

shop_urls = []
lock = Lock()
fail_count = 0
with open('/Users/xuegeng/Desktop/crawler/page_shop13.json','r') as f:#不用改，就是2，直接爬就行
    shop_pages  = json.load(f)
d = shop_pages.copy()   
for i in shop_pages.iteritems():
    if i[1]:del d[i[0]]
print len(d.keys())
def getHtmlText(url):
    '''return text-form html'''
    try:
        html = requests.get(url, headers = HEADER2, timeout = 5)
        html.encoding = 'utf-8'
        return html.text
    except:
        print "[-] Fail to get " + url
        return None
def getMaxPageNum(soup):
    '''return max page number of the result'''
    p = soup.find(class_ = "page")
    try:
        p_text = p.get_text()
        digits = list(filter(lambda x:x.isdigit(),p_text.split('\n')))
        max_page_num = max(map(int,digits))
    except AttributeError:
        max_page_num = 1
    return max_page_num
    
def getPageShops(page_url):
    '''return a list of shop url'''
    domain = 'https://www.dianping.com'
    link_filter = '//a[@data-hippo-type="shop"]/@href'
    html_text = getHtmlText(page_url)
    if not html_text:return []
    html = getEtreeHtml(html_text)
    title = html.xpath('/html/head/title')[0].text
    print '[+] Crawling ' + title + ' ' + page_url
    result = html.xpath(link_filter)
    
    return map(lambda x:domain+str(x),result)
    
def getPagesShopUrls(first_url):
    '''return all the shop urls within the first page's restriction'''
    global shop_urls,lock
    html_text = getHtmlText(first_url)
    max_page = getMaxPageNum(getSoup(html_text))
    pages = map(lambda x:x+1,range(max_page))
    page_urls = map(lambda x : first_url + 'p' + str(x) , pages)
    for page_url in page_urls:
        page_shop_urls = getPageShops(page_url)
        with lock:
            shop_urls.extend(page_shop_urls)
            print '[=] Shop urls length: ' + str(len(shop_urls))

def mergeIntoTheResult(page_url):
    global shop_pages,shop_urls,lock
    result = getPageShops(page_url)
    if result:
        with lock:
            shop_pages[page_url] = True
            shop_urls.extend(result)

def processMerging():
    '''to have process handle shop url crawling'''
    global shop_pages,shop_urls
    pool = Pool(processes = 10)
    results = pool.map(mergeIntoTheResult, d.keys())
    pool.close()
    pool.join()
    with open('/Users/xuegeng/Desktop/crawler/page_shop14.json','w') as f:
        json.dump(shop_pages,f)


#processMerging()
#with open('/Users/xuegeng/Desktop/crawler/page_shop14.json','w') as f:
#        json.dump(shop_pages,f)
#with open('/Users/xuegeng/Desktop/crawler/shop_urls.json','r') as f:
#        l = json.load(f)
#with open('/Users/xuegeng/Desktop/crawler/shop_urls.json','w') as f:
#    shop_urls.extend(l)
#    json.dump(shop_urls,f)
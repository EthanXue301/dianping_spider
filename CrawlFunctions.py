# -*- coding: utf-8 -*-

"""
this script contains frequently used functions
"""

import requests,re
from lxml import etree
from bs4 import BeautifulSoup
from CONSTANTS import HEADER


URLS = []
def getHtmlText(url):
    '''return text-form html'''
    global URLS
    if url in URLS:return None
    try:
        html = requests.get(url, headers = HEADER, timeout = 5)
        html.encoding = 'utf-8'
        return html.text
    except:return None

def getSoup(html_text):
    '''return a BeautifulSoup Object'''
    return BeautifulSoup(html_text, 'html.parser', from_encoding='utf-8')

def getEtreeHtml(html_text):
    '''return a HTML Object'''
    return etree.HTML(html_text)

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
    
def getRegionUrls(html):
    '''return a list of url of a certain region'''
    domain = 'https://www.dianping.com'
    ft = '//*[@id="region-nav"]/a'
    result = html.xpath(ft)
    urls = map(lambda x : domain + x.values()[0] , result)
    return urls

def getSubRegionUrls(html):
    '''return a list of url of a certain sub-region'''
    domain = 'https://www.dianping.com'
    ft = '//*[@id="region-nav-sub"]/a'
    result = html.xpath(ft)
    urls = map(lambda x : domain + x.values()[0] , result)
    return urls

def getSubRegionCuisineUrls(html):
    '''return a list of url of a certain sub-region'''
    domain = 'https://www.dianping.com'
    ft = '//*[@id="classfy"]/a'
    result = html.xpath(ft)
    urls = map(lambda x : domain + x.values()[0] , result)
    return urls
  
def getSubRegionSubCuisineUrls(html):
    '''return a list of url of a certain sub-region'''
    domain = 'https://www.dianping.com'
    ft = '//*[@id="classfy-sub"]/a'
    result = html.xpath(ft)
    urls = map(lambda x : domain + x.values()[0] , result)
    return urls
      
def getAllSubRegionCuisineUrls():
    '''crawl all the necessary url'''
    global HEADER,URLS
    #从一个初始页面开始
    start_url = 'https://www.dianping.com/search/category/2/10'
    start_html_text = getHtmlText(start_url)
    start_html = getEtreeHtml(start_html_text)
    all_region = getRegionUrls(start_html)
    #对于一个区域，如果页数大于50，那就爬取副区域，跳到副区域
    for region in all_region:
        region_html_text = getHtmlText(region)
        if not region_html_text:continue
        region_soup = getSoup(region_html_text)
        region_max_page = getMaxPageNum(region_soup)
        region_html = getEtreeHtml(region_html_text)
        print "[+] Crawling Region " + region_html.xpath('/html/head/title')[0].text + " Max Page: " + str(region_max_page)
        if region_max_page < 50:
            URLS.append((region,region_max_page))
        else:
            #对于一个副区域，如果页数大于50，那就爬取菜式，跳到副区域+菜式
            subRegions = getSubRegionUrls(region_html)
            for subRegion in subRegions:
                if subRegion == region : continue#因为有一个 不限 选项
                subregion_html_text = getHtmlText(subRegion)
                if not subregion_html_text:continue
                subregion_soup = getSoup(subregion_html_text)
                subregion_max_page = getMaxPageNum(subregion_soup)
                subregion_html = getEtreeHtml(subregion_html_text)
                print "[+] Crawling Sub-Region " + subregion_html.xpath('/html/head/title')[0].text + " Max Page: " + str(subregion_max_page)
                if subregion_max_page < 50:
                    URLS.append((subRegion,subregion_max_page))
                else:
                    #对于一个菜式，如果页数大于50，那就爬取副菜式，跳到副区域+副菜式
                    cuisines = getSubRegionCuisineUrls(subregion_html)
                    for cuisine in cuisines:
                        cuisine_html_text = getHtmlText(cuisine)
                        if not cuisine_html_text:continue
                        cuisine_soup = getSoup(cuisine_html_text)
                        cuisine_max_page = getMaxPageNum(cuisine_soup)
                        cuisine_html = getEtreeHtml(cuisine_html_text)
                        print "[+] Crawling Sub-Region Cuisine " + cuisine_html.xpath('/html/head/title')[0].text + " Max Page: " + str(cuisine_max_page)
                        if cuisine_max_page < 50:
                            URLS.append((cuisine,cuisine_max_page))
                        else:
                            #对于一个副菜式，如果还大于50，那就没有办法了
                            for subcuisine in getSubRegionSubCuisineUrls(cuisine_html):
                                if subcuisine == cuisine : continue
                                subcuisine_html_text = getHtmlText(subcuisine)
                                if not subcuisine_html_text:continue
                                subcuisine_soup = getSoup(subcuisine_html_text)
                                subcuisine_max_page = getMaxPageNum(subcuisine_soup)
                                subcuisine_html = getEtreeHtml(subcuisine_html_text)
                                print "[+] Crawling Sub-Region Sub-Cuisine " + subcuisine_html.xpath('/html/head/title')[0].text + " Max Page: " + str(subcuisine_max_page)
                                URLS.append((subcuisine,subcuisine_max_page))
    
def getAllSubRegionCuisineUrls2():
    '''crawl all the necessary url'''
    global HEADER,URLS
    #从一个初始页面开始
    #start_url = 'https://www.dianping.com/search/category/2/10'
    #start_html_text = getHtmlText(start_url)
    #start_html = getEtreeHtml(start_html_text)
    #all_region = getRegionUrls(start_html)
    #对于一个区域，如果页数大于50，那就爬取副区域，跳到副区域
    for region in ['https://www.dianping.com/search/category/2/10/r27614','https://www.dianping.com/search/category/2/10/r27616']:
        region_html_text = getHtmlText(region)
        if not region_html_text:continue
        region_soup = getSoup(region_html_text)
        region_max_page = getMaxPageNum(region_soup)
        region_html = getEtreeHtml(region_html_text)
        print "[+] Crawling Region " + region_html.xpath('/html/head/title')[0].text + " Max Page: " + str(region_max_page)
        if region_max_page < 50:
            URLS.append((region,region_max_page))
        else:
                    #对于一个菜式，如果页数大于50，那就爬取副菜式，跳到副区域+副菜式
                    cuisines = getSubRegionCuisineUrls(region_html)
                    for cuisine in cuisines:
                        cuisine_html_text = getHtmlText(cuisine)
                        if not cuisine_html_text:continue
                        cuisine_soup = getSoup(cuisine_html_text)
                        cuisine_max_page = getMaxPageNum(cuisine_soup)
                        cuisine_html = getEtreeHtml(cuisine_html_text)
                        print "[+] Crawling Sub-Region Cuisine " + cuisine_html.xpath('/html/head/title')[0].text + " Max Page: " + str(cuisine_max_page)
                        if cuisine_max_page < 50:
                            URLS.append((cuisine,cuisine_max_page))
                        else:
                            #对于一个副菜式，如果还大于50，那就没有办法了
                            for subcuisine in getSubRegionSubCuisineUrls(cuisine_html):
                                if subcuisine == cuisine : continue
                                subcuisine_html_text = getHtmlText(subcuisine)
                                if not subcuisine_html_text:continue
                                subcuisine_soup = getSoup(subcuisine_html_text)
                                subcuisine_max_page = getMaxPageNum(subcuisine_soup)
                                subcuisine_html = getEtreeHtml(subcuisine_html_text)
                                print "[+] Crawling Sub-Region Sub-Cuisine " + subcuisine_html.xpath('/html/head/title')[0].text + " Max Page: " + str(subcuisine_max_page)
                                URLS.append((subcuisine,subcuisine_max_page))    
#getAllSubRegionCuisineUrls2()

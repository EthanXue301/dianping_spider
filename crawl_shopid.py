# -*- coding: utf-8 -*-

from CONSTANTS import HEADER
import requests,re,time
from re import findall
from bs4 import BeautifulSoup
import urllib2,optparse
import pandas as pd
URL = 'https://www.dianping.com/shop/'

def gethtml(url, headers):
    html = requests.get(url, headers = headers)
    html.encoding = 'utf-8'
    return html.text

head = {'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding':'gzip, deflate, sdch',
        'Accept-Language':'zh-CN,zh;q=0.8,en;q=0.6',
        'Cache-Control':'max-age=0',
        'Connection':'keep-alive',
        'Cookie':'_hc.v=1ab2c409-dc29-f1a5-e08d-9c6e2689a281.1480398303; __utma=1.1563952025.1480398303.1480398303.1480398303.1; __utmz=1.1480398303.1.1.utmcsr=baidu|utmccn=(organic)|utmcmd=organic; JSESSIONID=C4AA416773586D3C4FE08E52ED5E48DA; aburl=1; cy=2; cye=beijing',
        'Host':'www.dianping.com',
        'Upgrade-Insecure-Requests':'1',
        'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.95 Safari/537.36'}



def html2df(shopid = '20942012'):
    shopList = []
    url = URL + shopid
    html = gethtml(url, head)
    soup = BeautifulSoup(html, 'html.parser', from_encoding='utf-8')
    
    shopname= soup.find(class_ = "shop-name").get_text()
    city = soup.find(class_ = "city J-city").get_text()
    print shopname,city
    
    p = re.compile(r'\d+')
    for link in soup.findAll('a', attrs={'href': re.compile(".*/shop/.*")}):
        shopList.append(p.findall(link.get('href'))[0])
    return shopList
#shopList = html2df()
#allSet = set(shopList)
#usedSet = set('20942012')
#waitedSet = allSet.difference(usedSet)
#
#try:
#    while True:
#        waitedSet = allSet.difference(usedSet)
#        #从等待集合中挑选出一个id
#        shopID = waitedSet.pop()
#        #添加到已经使用的集合
#        usedSet.add(shopID)
#        #爬取网页内的店铺id
#        time.sleep(1)
#        shopList = html2df(shopID)
#        #添加到总的集合
#        allSet = allSet.union(set(shopList))
#except:
#    import pickle
#    pickle.dump(allSet, open("allSet.xg", "w"))
    #obj2 = pickle.load(open("tmp.txt", "r"))
    







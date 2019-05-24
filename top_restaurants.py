# -*- coding: utf-8 -*-
import requests
import json
import os

headers = {
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:39.0) Gecko/20100101 Firefox/39.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive'}

def get_data(url):
    html=requests.get(url,headers=headers).text
    data=json.loads(html)['shopBeans']
    return data

def shoplist():
    try:
        os.mkdir('data')
    except:
        print('--')
    items={'最佳餐厅':'http://www.dianping.com/mylist/ajax/shoprank?cityId=%s&shopType=10&rankType=score&categoryId=0','人气餐厅':'http://www.dianping.com/mylist/ajax/shoprank?cityId=%s&shopType=10&rankType=popscore&categoryId=0','口味最佳':'http://www.dianping.com/mylist/ajax/shoprank?cityId=%s&shopType=10&rankType=score1&categoryId=0','环境最佳':'http://www.dianping.com/mylist/ajax/shoprank?cityId=%s&shopType=10&rankType=score2&categoryId=0','服务最佳':'http://www.dianping.com/mylist/ajax/shoprank?cityId=%s&shopType=10&rankType=score3&categoryId=0'}
    citys={'北京':'2','上海':'1','广州':'4','深圳':'7','成都':'8','重庆':'9','杭州':'3','南京':'5','沈阳':'18','苏州':'6','天津':'10','武汉':'16','西安':'17','长沙':'344','大连':'19','济南':'22','宁波':'11','青岛':'21','无锡':'13','厦门':'15','郑州':'160'}
    #excel=xlwt3.Workbook()
    #sheet=excel.add_sheet('sheet')
    count=0

    for key in items:
        try:
            data=get_data(items[key]%('2'))
        except:
            print('Error!')
            continue
        num=1
        for item in data:
            print item['filterFullName']
            print item['shopId']
            num+=1
            count+=1
    print count
shoplist()
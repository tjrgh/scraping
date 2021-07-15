# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
import os
from datetime import datetime

import pymongo


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')

import pandas as pd
import matplotlib.pyplot as plt


# aaa = pd.read_csv("C:/Users/kai/Downloads/air_quality_no2.csv", index_col=0, parse_dates=True)
# print(aaa)
# aaa.plot()
# plt.show()
# print('hehe')

# try문 실습
# def AAA():
#     a = 'hello'
#     try:
#         print(a+1);
#         print('hello');
#         return;
# except Exception as e:
#     print('in except : ');
#     print(e)
#     return;
# else:
#     print('in else');
# finally:
#     print('in finally');
#     1/0
# return;
# AAA();

# 클래스, 인스턴스 실습
# class BBB:
#     b1 = 1;
#
#     def bFunc(self):
#         self.b9 = 9;
#         print('in BBB"s bFunc()');


# bbb1 = BBB();
# bbb2 = BBB();
#
# bbb1.b2 = 2;
# bbb2.b3 = 3;
#
# bbb1.b1 = 99;
# print(bbb1.b1)

# 메타클래스
# def upper_attr(future_class_name, future_class_parents, future_class_attr):
#   """
#     대문자로 변환된 속성의 리스트와 함께 클래스 객체를 반환합니다.
#   """
#
#   # '__'로 시작하지 않는 모든 객체를 가져와 대문자로 변환합니다.
#   uppercase_attr = {}
#   for name, val in future_class_attr.items():
#       if not name.startswith('__'):
#           uppercase_attr[name.upper()] = val
#       else:
#           uppercase_attr[name] = val
#
#   # `type`으로 클래스를 생성합니다.
#   # return type(future_class_name, future_class_parents, uppercase_attr)
#   return BBB;
#
# class UpperAttrMetaclass(type):
#     # __new__는 __init__가 호출되기 전에 먼저 호출되는 메소드입니다.
#     # 이 메소드는 실제 객체를 만들고 반환합니다.
#     # __init__가 넘겨받은 인자로 객체를 초기화하는데 반해
#     # 우리는 여기서 __new__를 사용합니다.
#     # 여기서 생성되는 객체는 클래스이고 우리는 해당 클래스를 커스텀하기 위해 __new__를
#     # 오버라이드해서 사용합니다.
#     # 원하는 추가적인 행동에 대해서는 __init__에서 할 수 있습니다.
#     # 몇몇 더 복잡한 사용 예에서는 __call__도 오버라이드해서 사용합니다
#     # 하지만 이 예에서는 하지 않습니다.
#     def __new__(upperattr_metaclass, future_class_name,
#                 future_class_parents, future_class_attr):
#
#         uppercase_attr = {}
#         for name, val in future_class_attr.items():
#             if not name.startswith('__'):
#                 uppercase_attr[name.upper()] = val
#             else:
#                 uppercase_attr[name] = val
#         print('in metaclass"s __new__');
#         return type(future_class_name, future_class_parents, uppercase_attr)
#
# # __metaclass__ = UpperAttrMetaclass # 이리하여 모듈 내에 있는 모든 클래스가 영향을 받게 됩니다.
#
# class Foo(metaclass=UpperAttrMetaclass): # 하지만 글로벌 메타클래스는 object와 함께 작동하지 않습니다
#   # 하지만 우리는 이 클래스에만 영향을 주고자 여기에 __metaclss__를 정의하면
#   # object 자식(children)과 함께 작동하게 됩니다.
#   bar = 'bip'
#
# class SubFoo(metaclass=upper_attr):
#     barrr = 'bippp'
#
# print(hasattr(Foo, 'bar'))
# # Out: False
# print(hasattr(Foo, 'BAR'))
# # Out: True
#
# f = Foo()
#
# print(f.__class__)
# print(f.__class__.__class__)
# print(f.BAR)
#
# subF = SubFoo()
# print(subF.__class__)
# print(subF.__class__.__class__)
# print(subF.BARRR)

# class AAA:
#     a =1;
#     def f1(self):
#         print("in AAA's f1()");
#     def f2(self):
#         print("in AAA's f2()");
#
#     def generator(self, data):
#         for i in range(0, len(data), 1):
#             if i%3 == 0:
#                 yield data[i]
#
#
# class BBB(AAA):
#     b =1 ;
#     def f1(self):
#         print("in BBB's f1()");
#     def f3(self):
#         AAA.f1(AAA);
#
# aaa = AAA()
# bbb = BBB()
#
# for i in aaa.generator(range(0, 10, 1)):
#     print(i)
#
# print(aaa.generator(range(0,10,1)).__class__.__class__)

# class AAA:
#     a = 1;
#     def f1(self):
#         print('hello');
#
# aaa = AAA();
# aaa.f1();
# aaa.a = 9;
# print(aaa.a); #9출력.
#
# print(AAA.a)

# import numpy as np
#
# a = np.array([1, 2, 3])
# b = np.array([4, 5, 6])
# print(a+b);
# a.max()
# a.sum()
#
# c2 = a * b
# print(c2)
#
# a = np.array([1, 2, 3])
# a = np.array([ [ [1, 2, 3],
#                  [4, 5, 6] ],
#                ])

from bs4 import BeautifulSoup
import requests

# url = 'https://deepsearch.com/analytics/company-analysis/KRX:005930?c-symbol=KRX%3A005930&c-name=%EC%82%BC%EC%84%B1%EC%A0%84%EC%9E%90';

# res = requests.get(url)
# print(res.text)
# soup = BeautifulSoup(res.text)
# print(soup.find('div', attrs={'class':'title'}))
# a = (soup.select_one('div.table-data'))

# data_list = soup.find_all('.contents-area .rt-tbody .rt-tr-group')
#
# for data in data_list:
#     print(data);
#     print('\n')
#
# url2 = 'https://www.naver.com/'
# res2 = requests.get(url2)
# soup2 = BeautifulSoup(res2.text)
# print(soup2.find('iframe', attrs={'id': 'shop_header'}))
# #
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

from bs4 import BeautifulSoup
import time

#로그인 필요없는 '기업분석'페이지를 selenium으로 조회.
# chrome_driver = 'C:/Users/kai/Desktop/chromedriver_win32/chromedriver.exe'
# driver = webdriver.Chrome(chrome_driver)
#
# driver.get('https://deepsearch.com/analytics/company-analysis/KRX:005930?c-symbol=KRX%3A005930&c-name=%EC%82%BC%EC%84%B1%EC%A0%84%EC%9E%90');
# # time.sleep(5)
# # data = driver.find_element_by_id('shop_header')
# WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div.table-data')))
# data = driver.find_element_by_css_selector('div.table-data')
# print(data.text)
#
# # commentButton.click()
# #
# # commentList = driver.find_element_by_class_name('u_cbox_contents')
# # commentContent = commentList.
# # time.sleep(2)
# driver.close()


# url = "https://api.deepsearch.com/v1/compute";
# headers = {
#     ':authority': 'api.deepsearch.com',
#     ':method': 'POST',
#     ':path': '/v1/compute',
#     ':scheme': 'https',
#     'accept': 'application/json, text/plain, */*',
#     'accept-encoding': 'gzip, deflate, br',
#     'accept-language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
#     'authorization': 'Basic NDEwMTYzNzY2YzdjNGEzZGE4ZGRkY2VjODg4NmRjZWM6ZmY5MWFhMjA3MGQ4MzRkY2RmMzUyNmRhYTIxY2Q1MTE4NWQ3ZGU2MWMwZjRiZGJkZjQ0NzFiYmYxNjg5Zjg4Mg==',
#     'content-length': '136',
#     'content-type': 'application/json;charset=UTF-8',
#     'cookie': '_ga=GA1.2.128850437.1623665388; _fbp=fb.1.1623665387940.1302350526; ch-veil-id=e9c84462-cfe2-4a2b-a6da-01c0b4e8765f; _gid=GA1.2.113623208.1624238968; _gcl_au=1.1.686019624.1624238968; _ds_token=KZSsdkHlvfMQ9RmzwrpAOJGevgUuqLif6HrXvIs55GGi0YAi150Zjw66P6lDW7I4Khva5UCBFapLwOHq5RNeYQ; amp_9a6285_deepsearch.com=uT95rZXyx7sdHqf55DiyPW.MTA1ODQxMDc0NTc4MTQyNDEyOA==..1f8mkumqi.1f8mlqm34.t.l.1i; _gat=1; _gat_UA-118044144-2=1; ch-session-23738=eyJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJzZXMiLCJrZXkiOiIyMzczOC02MGM3MmFlYjRhODI2MDczNzM5YiIsImlhdCI6MTYyNDI2NjA0NCwiZXhwIjoxNjI2ODU4MDQ0fQ.Yvf0W3hdUHt69LVAOl-aDQacdDjHuM5oKeFye2qi1Eo; amp_9a6285=uT95rZXyx7sdHqf55DiyPW.MTA1ODQxMDc0NTc4MTQyNDEyOA==..1f8mspub3.1f8mspuis.v.b.1a',
#     'origin': 'https://deepsearch.com',
#     'referer': 'https://deepsearch.com/',
#     'sec-ch-ua': '" Not;A Brand";v="99", "Google Chrome";v="91", "Chromium";v="91"',
#     'sec-ch-ua-mobile': '?0',
#     'sec-fetch-dest': 'empty',
#     'sec-fetch-mode': 'cors',
#     'sec-fetch-site': 'same-site',
#     'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.101 Safari/537.36',
#     'x-deepsearch-app-id': 'deepsearch',
#     'x-deepsearch-encoded-input': 'true',
# }
# res = requests.get(url, headers)
# print(res.text)
# soup = BeautifulSoup(res.text)
# print(soup.find('div', attrs={'class':'title'}))
# print(soup.find('title'))
# data_list = soup.find_all('.contents-area .rt-tbody .rt-tr-group')
#
# for data in data_list:
#     print(data);
#     print('\n')
#
# url2 = 'https://www.naver.com/'
# res2 = requests.get(url2)
# soup2 = BeautifulSoup(res2.text)
# print(soup2.find('iframe', attrs={'id': 'shop_header'}))

#로그인이 필요한 '경제지표'페이지를 selenium으로 조회.
# chrome_driver = 'C:/Users/kai/Desktop/chromedriver_win32/chromedriver.exe'
# driver = webdriver.Chrome(chrome_driver)
# driver.implicitly_wait(1)
# driver.get('https://deepsearch.com/analytics/economic-indicator?pageView=1&symbol=BOK%3A036Y001.0000001&chartEditSetting=JTdCJTIyc2VyaWVzTGlzdCUyMiUzQSU1QiU3QiUyMnN5bWJvbCUyMiUzQSUyMkJPSyUzQTAzNlkwMDEuMDAwMDAwMSUyMiUyQyUyMmF4aXMlMjIlM0EwJTdEJTVEJTJDJTIybGVnZW5kUG9zaXRpb24lMjIlM0EyJTJDJTIybGVnZW5kT3JpZW50YXRpb24lMjIlM0ElMjJoJTIyJTdE')
# time.sleep(5)
# data = driver.find_element_by_id('shop_header')
# WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div.rt-resizable-header-content')))
# driver.implicitly_wait(1)
# data = driver.find_element_by_css_selector('div.rt-resizable-header-content')
# print(data.text)

# commentButton.click()
#
# commentList = driver.find_element_by_class_name('u_cbox_contents')
# commentContent = commentList.
# time.sleep(2)
# driver.close()

from scrapy import cmdline
# cmdline.execute("cd ".split())
# cmdline.execute("scrapy crawl noname".split())

# csv 파일 읽기
import csv
# with open('C:/Users/kai/Desktop/so_tags.csv', 'r') as csv_file:
#     reader = csv.reader(csv_file)
#     for row in reader:
#         print(row)

# csv_df = pd.read_csv('C:/Users/kai/Desktop/so_tags.csv')
# print(csv_df)

# excel_df = pd.read_excel('C:/Users/kai/Downloads/so_tags.xlsx')
# print(excel_df)

# 몽고디비 연결
# client = pymongo.MongoClient('localhost', 27017)
# db = client.noname
# stock_list = list(db.stock_list.find({}))
#
# item = pd.read_excel("C:/Users/kai/Desktop/stock_list.xlsx")
#
# post_id = stock_list.insert_many(item.to_dict("records"))

f = open('./noname/fail_list.txt','a', encoding="UTF-8");
f.write("hello2")

# postgresql 연결
# import psycopg2
# db = psycopg2.connect(host="112.220.72.178", dbname="openmetric", user="openmetric", password=")!metricAdmin01", port=2345)


# pl = pd.read_excel("C:/Users/kai/Downloads/"+kospi_list[0]["한글 종목약명"]+"-포괄손익계산서-분기(3개월)_연결.xlsx")
# bs = pd.read_excel("C:/Users/kai/Downloads/"+kospi_list[0]["한글 종목약명"]+"-재무상태표-분기(3개월)_연결.xlsx")
# cf = pd.read_excel("C:/Users/kai/Downloads/"+kospi_list[0]["한글 종목약명"]+"-현금흐름표-분기(3개월)_연결.xlsx")

# 손익계산서 저장




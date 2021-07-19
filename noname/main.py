# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
import os
import traceback
from datetime import datetime, timezone

import pymongo


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')

import pandas as pd
import matplotlib.pyplot as plt
from bs4 import BeautifulSoup
import requests
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import time
from scrapy import cmdline

# cmdline.execute("scrapy crawl korean_daily_finance_spider".split())
# cmdline.execute("scrapy crawl noname".split())
cmdline.execute("scrapy crawl report_spider".split())

# 분기 데이터 스크래핑 스케줄러.
from apscheduler.schedulers.background import BackgroundScheduler, BlockingScheduler

scheduler = BlockingScheduler()
def job(quarter):
    print("korean daily finance spider start")
    cmdline.execute(("scrapy crawl korean_daily_finance_spider -a quarter="+quarter).split())
# scheduler.add_job(job, 'interval', hours=24, start_date="2021-07-14 00:00:00", end_date="2021-12-31 00:00:00")

scheduler.add_job(job, 'interval', hours=24, args=["2021-03-31"])
scheduler.start()

def store_excel_data():
    # 몽고디비 연결
    client = pymongo.MongoClient('localhost', 27017)
    db = client.noname
    kospi_list = list(db.kospi_list.find({}))
    # item = pd.read_excel("C:/Users/kai/Downloads/KOSPI주식목록.xlsx")

    # post_id = kospi_list.insert_many(item.to_dict("records"))

    # postgresql 연결
    import psycopg2
    import numpy as np
    import re
    db = psycopg2.connect(host="112.220.72.178", dbname="openmetric", user="openmetric", password=")!metricAdmin01", port=2345)
    cur = db.cursor()

    # cur.execute("select * from article_post")
    # print(cur.fetchone())

    db_update_fail_list = open("./db_update_fail_list.txt","a")

    # 종목 리스트 반복
    for stock in kospi_list:
        #엑셀 데이터 DataFrame으로 가져오기.
        try:
            # 포괄손익계산서
            pl = pd.read_excel("C:/Users/kai/Downloads/" + stock["한글 종목약명"] + "-포괄손익계산서-분기(3개월)_연결.xlsx")
            # 재무상태표
            bs = pd.read_excel("C:/Users/kai/Downloads/" + stock["한글 종목약명"] + "-재무상태표-분기(3개월)_연결.xlsx")
            # 현금흐름표
            cf = pd.read_excel("C:/Users/kai/Downloads/" + stock["한글 종목약명"] + "-현금흐름표-분기(3개월)_연결.xlsx")
        except:
            date_time = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time()))
            db_update_fail_list.write(date_time + "_" + stock["단축코드"] + "_" + stock["한글 종목약명"] + "\n")
            continue

        # dataframe의 컬럼명 변경.
        pl.columns = pl.loc[0]
        pl = pl.drop([0])
        bs.columns = bs.loc[0]
        bs = bs.drop([0])
        cf.columns = cf.loc[0]
        cf = cf.drop([0])

        # 포괄손익계산서 저장.
        for row in pl.index: # 엑셀 파일의 row들에 대해 반복.
            # 컬럼에 대해 반복하여 분기를 나타내는 컬럼인 경우에 data insert.
            for i in pl.columns:
                if None != re.search(r"\d{4}-\d{2}-\d{2}", i):# yyyy-MM-dd형태의 컬럼명이면 분기데이터 컬럼으로 판단. data insert.
                    cur.execute("INSERT INTO stock_financial_statement ("
                                "created_at, updated_at, corp_code, business_year, business_month, this_term_name, subject_name, account_id, account_name, "
                                "account_level, this_term_amount, ordering) "
                                "VALUES ("
                                "'"+str(datetime.now(timezone.utc))+"', '"+str(datetime.now(timezone.utc))+"', "
                                "'???', '???', '???', '"+i+"', '포괄손익계산서', '"+str(pl.loc[row]["account_id"])+"', "
                                "'"+pl.loc[row]["계정명"]+"', '"+str(pl.loc[row]["LV"])+"', '"+str(pl.loc[row][i])+"', '"+str(pl.loc[row]["LV"])+"') ")
                    db.commit()

        # 재무상태표 저장
        for row in bs.index: # 엑셀 파일의 row들에 대해 반복.
            # 컬럼에 대해 반복하여 분기를 나타내는 컬럼인 경우에 data insert.
            for i in bs.columns:
                if None != re.search(r"\d{4}-\d{2}-\d{2}", i):# yyyy-MM-dd형태의 컬럼명이면 분기데이터 컬럼으로 판단. data insert.
                    # amount = bs.loc[row][i]
                    # if np.isnan(bs.loc[row][i]) :
                    #     amount = 0
                    cur.execute("INSERT INTO stock_financial_statement ("
                                "created_at, updated_at, corp_code, business_year, business_month, this_term_name, subject_name, account_id, account_name, "
                                "account_level, this_term_amount, ordering) "
                                "VALUES ("
                                "'"+str(datetime.now(timezone.utc))+"', '"+str(datetime.now(timezone.utc))+"', "
                                "'???', '???', '???', '"+i+"', '재무상태표', '"+str(bs.loc[row]["account_id"])+"', "
                                "'"+bs.loc[row]["계정명"]+"', '"+str(bs.loc[row]["LV"])+"', '"+str(bs.loc[row][i])+"', '"+str(bs.loc[row]["LV"])+"') ")
                    db.commit()

        # 현금흐름표 저장.
        for row in cf.index: # 엑셀 파일의 row들에 대해 반복.
            # 컬럼에 대해 반복하여 분기를 나타내는 컬럼인 경우에 data insert.
            for i in cf.columns:
                if None != re.search(r"\d{4}-\d{2}-\d{2}", i):# yyyy-MM-dd형태의 컬럼명이면 분기데이터 컬럼으로 판단. data insert.
                    cur.execute("INSERT INTO stock_financial_statement ("
                                "created_at, updated_at, corp_code, business_year, business_month, this_term_name, subject_name, account_id, account_name, "
                                "account_level, this_term_amount, ordering) "
                                "VALUES ("
                                "'"+str(datetime.now(timezone.utc))+"', '"+str(datetime.now(timezone.utc))+"', "
                                "'???', '???', '???', '"+i+"', '현금흐름표', '"+str(cf.loc[row]["account_id"])+"', "
                                "'"+cf.loc[row]["계정명"]+"', '"+str(cf.loc[row]["LV"])+"', '"+str(cf.loc[row][i])+"', '"+str(cf.loc[row]["LV"])+"') ")
                    db.commit()



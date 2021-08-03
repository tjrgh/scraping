# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
import multiprocessing
import os.path
import subprocess
import sys
import traceback
from datetime import datetime, timezone
from multiprocessing import freeze_support

import scrapy.crawler


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')
    freeze_support()

    import pandas as pd
    # import matplotlib.pyplot as plt
    # from bs4 import BeautifulSoup
    # import requests
    # from selenium import webdriver
    # from selenium.webdriver.common.keys import Keys
    # from selenium.webdriver.support.ui import WebDriverWait
    # from selenium.webdriver.support import expected_conditions as EC
    # from selenium.webdriver.common.by import By
    # from bs4 import BeautifulSoup
    import time
    from scrapy import cmdline
    import psycopg2
    import numpy as np
    import re
    import schedule_job

    # cmdline.execute("scrapy crawl korean_daily_finance_spider -a quarter=2021-06-30".split())
    # cmdline.execute("scrapy crawl noname".split())
    # cmdline.execute("scrapy crawl report_spider".split())
    # cmdline.execute("scrapy crawl notice_spider".split())
    # cmdline.execute("scrapy crawl sector_spider".split())
    # cmdline.execute("scrapy crawl theme_spider -a target_term=2020-12-31 -a pre_target_term=2019-12-31".split())
    # cmdline.execute("scrapy crawl social_keyword_spider".split())

    # 분기 데이터 스크래핑 스케줄러.
    from apscheduler.schedulers.background import BackgroundScheduler, BlockingScheduler

    scheduler = BlockingScheduler()

    def create_sector_check_process():
        print("create_sector_check_process")
        process = multiprocessing.Process(target=schedule_job.sector_scraping_check)
        process.start()
        process.join()
    def create_daily_check_process():
        print("create_daily_check_process")
        process = multiprocessing.Process(target=schedule_job.daily_check)
        process.start()
        process.join()

    # scheduler.add_job(daily_check, 'cron', hour='01', minute='00')
    # scheduler.add_job(sector_scraping_check, 'cron', hour='01', minute='00')
    # subprocess.Popen("scrapy crawl report_spider".split(), shell=True)
    scheduler.add_job(create_sector_check_process, 'cron', hour=2, minute=0)
    scheduler.add_job(create_daily_check_process, 'cron', hour=4, minute=30)
    scheduler.start()


    # 종목 재무 엑셀 파일 다운 확인.
    # stock_list = pd.read_excel("C:/Users/kai/Desktop/stock_list.xlsx")
    # db = psycopg2.connect(host="112.220.72.179", dbname="openmetric", user="openmetric",
    #                               password=")!metricAdmin01", port=2345)
    # cur = db.cursor()

    # self.cur.execute("select * from stocks_basic_info where corp_code != ' '")
    # self.kospi_list = self.cur.fetchall()
    # stock_list = pd.read_sql("select * from stocks_basic_info where corp_code!=' '", db)
    #
    # for index, stock in stock_list.iterrows():
    #     if (os.path.isfile("C:/Users/kai/Downloads/" + stock["name"] + "-포괄손익계산서-분기(3개월)_연결.xlsx") == True
    #         and os.path.isfile("C:/Users/kai/Downloads/" + stock["name"] + "-재무상태표-분기(3개월)_연결.xlsx") == True
    #         and os.path.isfile("C:/Users/kai/Downloads/" + stock["name"] + "-현금흐름표-분기(3개월)_연결.xlsx") == True):
    #         pass
    #     else:
    #         with open("./no_data_stock_list.txt", "a", encoding="UTF-8") as f:
    #             f.write(stock["code"]+"_"+stock["name"]+"\n")

    def store_excel_data():
        # 몽고디비 연결
        # client = pymongo.MongoClient('localhost', 27017)
        # db = client.noname
        # kospi_list = list(db.kospi_list.find({}))
        kospi_list = pd.read_excel("C:/Users/kai/Desktop/stock_list.xlsx",dtype={"단축코드":"str"})

        # post_id = kospi_list.insert_many(item.to_dict("records"))

        # postgresql 연결
        db = psycopg2.connect(host="112.220.72.179", dbname="openmetric", user="openmetric", password=")!metricAdmin01", port=2345)
        cur = db.cursor()

        # cur.execute("select * from article_post")
        # print(cur.fetchone())

        # kospi_list = kospi_list[1657:]

        # 종목 리스트 반복
        for index, stock in kospi_list.iterrows():
            stock["단축코드"] = "000040"
            stock["한글 종목약명"] = "KR모터스"

            # 이미 인서트 되었는지 확인
            cur.execute("select * from stock_financial_statement where code_id='A"+stock["단축코드"]+"'")
            pre_data = cur.fetchone()
            if pre_data != None:
                continue

            #엑셀 데이터 DataFrame으로 가져오기.
            try:
                # 포괄손익계산서
                pl = pd.read_excel("C:/Users/kai/Downloads/" + stock["한글 종목약명"] + "-포괄손익계산서-분기(3개월)_연결.xlsx",
                                   dtype={"단축코드":"str"})
                # 재무상태표
                bs = pd.read_excel("C:/Users/kai/Downloads/" + stock["한글 종목약명"] + "-재무상태표-분기(3개월)_연결.xlsx",
                                   dtype={"단축코드":"str"})
                # 현금흐름표
                cf = pd.read_excel("C:/Users/kai/Downloads/" + stock["한글 종목약명"] + "-현금흐름표-분기(3개월)_연결.xlsx",
                                   dtype={"단축코드":"str"})

                # dataframe의 컬럼명 변경.
                pl.columns = pl.loc[0]
                pl = pl.drop([0])
                bs.columns = bs.loc[0]
                bs = bs.drop([0])
                cf.columns = cf.loc[0]
                cf = cf.drop([0])

                # corp_code가져오기
                cur.execute("select corp_code from stocks_basic_info where code='A"+str(stock["단축코드"])+"' ")
                corp_code = cur.fetchone()[0]

                def insert_db(df, file_type):
                    insert_sql = ""
                    # 포괄손익계산서 저장.
                    for row in df.index:  # 엑셀 파일의 row들에 대해 반복.
                        # 컬럼에 대해 반복하여 분기를 나타내는 컬럼인 경우에 data insert.
                        for i in df.columns:
                            if None != re.search(r"\d{4}-\d{2}-\d{2}", i):  # yyyy-MM-dd형태의 컬럼명이면 분기데이터 컬럼으로 판단. data insert.
                                # this_term_amount
                                amount = df.loc[row][i]
                                if np.isnan(df.loc[row][i]):
                                    amount = 'null'
                                # account_name
                                account_name = df.loc[row]["계정명"].replace(" ", "")
                                lv = int(df.loc[row]["LV"])
                                row_sub_count = 1
                                while lv != 0:
                                    if lv - 1 != int(df.loc[row - row_sub_count]["LV"]):
                                        row_sub_count = row_sub_count + 1
                                        continue
                                    lv = int(df.loc[row - row_sub_count]["LV"])
                                    account_name = df.loc[row - row_sub_count]["계정명"].replace(" ", "") + "_" + account_name
                                    row_sub_count = row_sub_count + 1

                                sql_value = ("("+
                                    "'"+str(datetime.now(timezone.utc))+"', '"+str(datetime.now(timezone.utc))+"', "+
                                    "'"+corp_code+"', '"+i.split("-")[0]+"', '"+i.split("-")[1]+"', '"+i+"', '"+file_type+"', "+
                                    "'"+str(df.loc[row]["account_id"])+"', '"+account_name+"', '"+str(df.loc[row]["LV"])+"', "+
                                    ""+str(amount)+", '"+str(df.loc[row]["LV"])+"', 'A"+str(stock["단축코드"])+"')")
                                insert_sql = insert_sql + ", "+sql_value

                                # cur.execute("INSERT INTO stock_financial_statement ("
                                #             "created_at, updated_at, corp_code, business_year, business_month, this_term_name, "
                                #             "subject_name, account_id, account_name, "
                                #             "account_level, this_term_amount, ordering, code_id) "
                                #             "VALUES ('"+
                                #                 str(datetime.now(timezone.utc)) + "', '" +
                                #                 str( datetime.now(timezone.utc)) + "', "
                                #                 "'" + corp_code + "', '" + i.split("-")[0] + "', '" +
                                #                 i.split("-")[1] + "', '" + i + "', '" + file_type + "', '" +
                                #                 str(df.loc[row]["account_id"]) + "', "
                                #                 "'" + account_name + "', '" + str(df.loc[row]["LV"]) + "', " +
                                #                 str(amount) + ", '" + str(df.loc[row]["LV"]) + "', 'A" + stock["단축코드"] + "') ")

                    insert_sql = insert_sql[1:]
                    # cur.execute(insert_sql)
                    cur.execute("INSERT INTO stock_financial_statement ("
                                "created_at, updated_at, corp_code, business_year, business_month, this_term_name, "
                                "subject_name, account_id, account_name, "
                                "account_level, this_term_amount, ordering, code_id) "
                                "VALUES "+insert_sql)
                    db.commit()

                insert_db(pl, "포괄손익계산서")
                insert_db(bs, "재무상태표")
                insert_db(cf, "현금흐름표")
            except Exception as e:
                date_time = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time()))
                with open("./db_update_fail_list.txt", "a", encoding="UTF-8") as f:
                    f.write(date_time + "_" + str(stock["단축코드"]) + "_" + stock["한글 종목약명"])
                    f.write(traceback.format_exc()+"\n")
                continue

    # store_excel_data()

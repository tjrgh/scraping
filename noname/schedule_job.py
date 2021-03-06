from datetime import datetime, date, timedelta
import subprocess
import time

import pandas as pd
import psycopg2
from scrapy import cmdline
# C:\QseeScraping\Scripts\python.exe C:\QseeScraping\Scripts\qsee_scraper\getHistoricalData.py
def test():
    print("test job start")
    cmdline.execute("scrapy crawl naver_finance".split())

def sector_scraping_check():
    today = time.localtime(time.time())
    if today.tm_mon == 5 & today.tm_mday==18:
        cmdline.execute("scrapy crawl sector_spider -a target_term="+str(today.tm_year)+"-03-31 "
                                                   "-a pre_target_term="+str(today.tm_year-1)+"-12-31".split())
    elif today.tm_mon == 8 & today.tm_mday == 17:
        cmdline.execute("scrapy crawl sector_spider -a target_term=" + str(today.tm_year) + "-06-30 "
                                                                                            "-a pre_target_term=" + str(
            today.tm_year) + "-03-31".split())
    elif today.tm_mon == 11 & today.tm_mday == 16:
        cmdline.execute("scrapy crawl sector_spider -a target_term=" + str(today.tm_year) + "-09-30 "
                                                                                            "-a pre_target_term=" + str(
            today.tm_year) + "-06-30".split())
    elif today.tm_mon == 4 & today.tm_mday == 1:
        cmdline.execute("scrapy crawl sector_spider -a target_term=" + str(today.tm_year-1) + "-12-31 "
                                                                                            "-a pre_target_term=" + str(
            today.tm_year-1) + "-09-30".split())

def theme_scraping_check():
    today = time.localtime(time.time())
    if today.tm_mon == 5 & today.tm_mday == 18:
        cmdline.execute("scrapy crawl theme_spider -a target_term=" + str(today.tm_year) + "-03-31 "
                                                                                           "-a pre_target_term=" + str(
            today.tm_year - 1) + "-12-31".split())
    elif today.tm_mon == 8 & today.tm_mday == 17:
        cmdline.execute("scrapy crawl theme_spider -a target_term=" + str(today.tm_year) + "-06-30 "
                                                                                           "-a pre_target_term=" + str(
            today.tm_year) + "-03-31".split())
    elif today.tm_mon == 11 & today.tm_mday == 16:
        cmdline.execute("scrapy crawl theme_spider -a target_term=" + str(today.tm_year) + "-09-30 "
                                                                                           "-a pre_target_term=" + str(
            today.tm_year) + "-06-30".split())
    elif today.tm_mon == 4 & today.tm_mday == 1:
        cmdline.execute("scrapy crawl theme_spider -a target_term=" + str(today.tm_year - 1) + "-12-31 "
                                                                                               "-a pre_target_term=" + str(
            today.tm_year - 1) + "-09-30".split())

def daily_check():

    today = time.localtime(time.time())
    if (datetime.now() >= datetime(today.tm_year,5,18)) & (datetime.now() < datetime(today.tm_year,8, 17)):
        cmdline.execute(
            ("scrapy crawl korean_daily_finance_spider -a quarter="+str(today.tm_year)+"-03-31").split())

    elif (datetime.now() >= datetime.date(today.tm_year,8,17)) & (datetime.now() < datetime(today.tm_year,11, 16)):
        cmdline.execute(
            ("scrapy crawl korean_daily_finance_spider -a quarter=" + str(today.tm_year) + "-06-30").split())

    elif datetime.now() >= datetime(today.tm_year,11, 16):
        cmdline.execute(
            ("scrapy crawl korean_daily_finance_spider -a quarter=" + str(today.tm_year) + "-09-30").split())
    elif datetime.now() < datetime(today.tm_year,4, 1):
        cmdline.execute(
            ("scrapy crawl korean_daily_finance_spider -a quarter=" + str(today.tm_year-1) + "-09-30").split())

    elif (datetime.now() >= datetime(today.tm_year,4, 1)) & (datetime.now() < datetime(today.tm_year,5, 18)):
        cmdline.execute(
            ("scrapy crawl korean_daily_finance_spider -a quarter=" + str(today.tm_year-1) + "-12-31").split())

    # ?????? ?????? ????????? ?????? ?????? ?????? ????????? ????????????.
    # ?????? ????????? ????????? ?????? ?????? ?????? ????????? ????????????.
    #   ?????? ?????? ?????????, ?????? ?????? ?????? ????????? ???????????????, ?????? ????????? ?????? ????????? ???????????? ?????????.
    # ~~~~~


    # ????????? ?????? ?????? ?????? ??????
    # pre_list = pd.read_excel("C:/Users/kai/Desktop/quarterly_data_list_"+pre_quarterly_date+".xlsx")
    # if len(pre_list.index) != 0: # ???????????? ??? ???????????? ???????????????,
    # cmdline.execute(("scrapy crawl korean_daily_finance_spider -a quarter="+pre_quarterly_date).split())

    # last_list = pd.read_excel("C:/Users/kai/Desktop/quarterly_data_list_"+quarterly_date+".xlsx")
    # if len(last_list.index) != 0:
    # cmdline.execute(("scrapy crawl korean_daily_finance_spider -a quarter="+quarterly_date).split())

def social_keyword_scraping():
    # ??? ?????? ???????????????, ???????????? ????????? ??????, ???????????? ????????? ????????? ??????????????? ???.
    start_date = ""
    end_date = ""
    term_type = ""
    scraping_count_goal=350

    # ?????? ??????
    today = date.today()
    start_date = today - timedelta(days=today.weekday()) - timedelta(days=7)
    start_date = start_date.isoformat()
    end_date = today - timedelta(days=today.weekday()) - timedelta(days=1)
    end_date = end_date.isoformat()

    # ?????? ?????? ??????
    term_type = "W"

    # ?????? ???????????? ?????? ?????? ??????
    db = psycopg2.connect(host="112.220.72.179", dbname="openmetric", user="openmetric",
                               password=")!metricAdmin01", port=2345)
    cur = db.cursor()
    kospi_list = pd.read_sql("select * from stocks_basic_info where corp_code!=' '", db).sort_values(
        by="code")
    scraping_count_goal = len(kospi_list)/7 + 20

    # cmdline.execute(("scrapy crawl social_keyword_spider -a start_date="+start_date+" -a end_date="+end_date+\
    #                 " -a term_type="+term_type+" -a scraping_count_goal="+str(scraping_count_goal)).split())
    cmdline.execute("scrapy crawl social_keyword_spider -a start_date=2021-01-01 -a end_date=2021-06-30"
                    " -a term_type=H -a scraping_count_goal=350".split())

def big_kinds_news_scraping():
    # ????????? ???~???????????? ?????? ???????????? ?????? ????????????.

    start_date = ""
    end_date = ""
    scraping_count_goal = 0

    # ?????? ??????
    today = date.today()
    start_date = today - timedelta(days=today.weekday()) - timedelta(days=7)
    start_date = start_date.isoformat()
    end_date = today - timedelta(days=today.weekday()) - timedelta(days=1)
    end_date = end_date.isoformat()

    # ?????? ???????????? ?????? ?????? ??????
    db = psycopg2.connect(host="112.220.72.179", dbname="openmetric", user="openmetric",
                          password=")!metricAdmin01", port=2345)
    cur = db.cursor()
    kospi_list = pd.read_sql("select * from stocks_basic_info where corp_code!=' '", db).sort_values(
        by="code")
    scraping_count_goal = len(kospi_list) / 7 + 20

    cmdline.execute(("scrapy crawl big_kinds_news_spider "
                    "-a start_date="+start_date+" -a end_date="+end_date+" "
                    "-a scraping_count_goal="+scraping_count_goal).split())

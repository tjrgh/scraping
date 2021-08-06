from datetime import datetime, date, timedelta
import subprocess
import time

import pandas as pd
import psycopg2
from scrapy import cmdline

def test():
    print("hello")
    # cmdline.execute("mkdir C:/Users/kai/Desktop/erere")
    cmdline.execute("scrapy crawl report_spider".split())

def sector_scraping_check():
    today = time.localtime(time.time())
    if today.tm_mon == 5 & today.tm_mday==18:
        cmdline.execute("scrapy crawl sector_spider -a target_term="+str(today.tm_year)+"-03-31 "
                                                   "-a pre_target_term="+str(today.tm_year-1)+"-12-31".split())
        cmdline.execute("scrapy crawl theme_spider -a target_term=" + str(today.tm_year) + "-03-31 "
                                                                                            "-a pre_target_term=" + str(
            today.tm_year - 1) + "-12-31".split())
    elif today.tm_mon == 8 & today.tm_mday == 17:
        cmdline.execute("scrapy crawl sector_spider -a target_term=" + str(today.tm_year) + "-06-30 "
                                                                                            "-a pre_target_term=" + str(
            today.tm_year) + "-03-31".split())
        cmdline.execute("scrapy crawl theme_spider -a target_term=" + str(today.tm_year) + "-06-30 "
                                                                                            "-a pre_target_term=" + str(
            today.tm_year) + "-03-31".split())
    elif today.tm_mon == 11 & today.tm_mday == 16:
        cmdline.execute("scrapy crawl sector_spider -a target_term=" + str(today.tm_year) + "-09-30 "
                                                                                            "-a pre_target_term=" + str(
            today.tm_year) + "-06-30".split())
        cmdline.execute("scrapy crawl theme_spider -a target_term=" + str(today.tm_year) + "-09-30 "
                                                                                            "-a pre_target_term=" + str(
            today.tm_year) + "-06-30".split())
    elif today.tm_mon == 4 & today.tm_mday == 1:
        cmdline.execute("scrapy crawl sector_spider -a target_term=" + str(today.tm_year-1) + "-12-31 "
                                                                                            "-a pre_target_term=" + str(
            today.tm_year-1) + "-09-30".split())
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

    # 신규 상장 종목에 대한 과거 재무 데이터 스크래핑.
    # 새로 추가된 종목에 대한 과거 재무 데이터 스크래핑.
    #   이게 선행 되어야, 최신 분기 재무 데이터 스크래핑시, 현재 분기에 대한 중복이 발생하지 않는다.
    # ~~~~~



    # 엑셀에 남은 종목 있나 체크
    # pre_list = pd.read_excel("C:/Users/kai/Desktop/quarterly_data_list_"+pre_quarterly_date+".xlsx")
    # if len(pre_list.index) != 0: # 확인해야 할 데이터가 남아있다면,
    # cmdline.execute(("scrapy crawl korean_daily_finance_spider -a quarter="+pre_quarterly_date).split())

    # last_list = pd.read_excel("C:/Users/kai/Desktop/quarterly_data_list_"+quarterly_date+".xlsx")
    # if len(last_list.index) != 0:
    # cmdline.execute(("scrapy crawl korean_daily_finance_spider -a quarter="+quarterly_date).split())

def social_keyword_scraping():
    # 주 단위 스크래핑시, 시작일은 월요일 날짜, 종료일은 일요일 날짜를 입력하여야 함.
    start_date = ""
    end_date = ""
    term_type = ""
    scraping_count_goal=350

    # 기간 설정
    today = date.today()
    start_date = today - timedelta(days=today.weekday()) - timedelta(days=7)
    start_date = start_date.isoformat()
    end_date = today - timedelta(days=today.weekday()) - timedelta(days=1)
    end_date = end_date.isoformat()

    # 기간 단위 설정
    term_type = "W"

    # 하루 스크래핑 종목 개수 설정
    db = psycopg2.connect(host="112.220.72.179", dbname="openmetric", user="openmetric",
                               password=")!metricAdmin01", port=2345)
    cur = db.cursor()
    kospi_list = pd.read_sql("select * from stocks_basic_info where corp_code!=' '", db).sort_values(
        by="code")
    scraping_count_goal = len(kospi_list)/7 + 20

    # cmdline.execute(("scrapy crawl social_keyword_spider -a start_date="+start_date+" -a end_date="+end_date+\
    #                 " -a term_type="+term_type+" -a scraping_count_goal="+scraping_count_goal).split())
    cmdline.execute("scrapy crawl social_keyword_spider -a start_date=2021-01-01 -a end_date=2021-06-30"
                    " -a term_type=H -a scraping_count_goal=350".split())
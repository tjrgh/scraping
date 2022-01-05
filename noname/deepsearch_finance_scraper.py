from datetime import datetime, date, timedelta
import subprocess
import time

import pandas as pd
import psycopg2
from scrapy import cmdline


today = time.localtime(time.time())
if (datetime.now() >= datetime(today.tm_year,5,18)) & (datetime.now() < datetime(today.tm_year,8, 17)):
    cmdline.execute(
        ("scrapy crawl korean_daily_finance_spider -a quarter="+str(today.tm_year)+"-03-31").split())

elif (datetime.now() >= datetime(today.tm_year,8,17)) & (datetime.now() < datetime(today.tm_year,11, 16)):
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
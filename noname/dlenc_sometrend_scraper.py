
# 주 단위 스크래핑시, 시작일은 월요일 날짜, 종료일은 일요일 날짜를 입력하여야 함.
# is_followed=True인 경우, start_date, end_date는 주 단위이어야 하며,
# is_followed=False인 경우, start_date, end_date는 간격이 1년 안이어야 한다.
import time
from datetime import date, timedelta

import pandas as pd
import psycopg2
from scrapy import cmdline

start_date = ""
end_date = ""
term_type = ""
scraping_count_goal=0
is_followed="True"
follow_start_year = 2021
use_site = "dlenc"

# 기간 설정
today = date.today()
start_date = today - timedelta(days=today.weekday()) - timedelta(days=7)
start_date = start_date.isoformat()
end_date = today - timedelta(days=today.weekday()) - timedelta(days=1)
end_date = end_date.isoformat()
end_date = "2021-12-31"

# 기간 단위 설정
term_type = "W"

# 하루 스크래핑 종목 개수 설정
db = psycopg2.connect(host="112.220.72.179", dbname="openmetric", user="openmetric",
                           password=")!metricAdmin01", port=2345)
cur = db.cursor()
kospi_list = pd.read_sql("select * from stocks_basic_info where corp_code!=' '", db).sort_values(
    by="code")
scraping_count_goal = int(len(kospi_list)/7) + 20

cmdline.execute(("scrapy crawl social_search_spider -a is_followed="+is_followed+" -a start_date="+start_date+" -a end_date="+end_date+" "
                "-a scraping_count_goal="+str(scraping_count_goal)+" -a follow_start_year="+str(follow_start_year)+" "
                "-a use_site="+use_site).split())
# try:
#     cmdline.execute(("scrapy crawl social_search_spider -a is_followed=True -a start_date=2021-09-20 -a end_date=2021-09-26 "
#                     "-a scraping_count_goal=150").split())
# except Exception as e:
#     time.sleep(10)
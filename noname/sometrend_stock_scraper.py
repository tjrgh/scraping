

# 주 단위 스크래핑시, 시작일은 월요일 날짜, 종료일은 일요일 날짜를 입력하여야 함.
from datetime import date, timedelta

import pandas as pd
import psycopg2
from scrapy import cmdline

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
scraping_count_goal = int(len(kospi_list)/7) + 20

# cmdline.execute(("scrapy crawl social_keyword_spider -a start_date="+start_date+" -a end_date="+end_date+\
#                 " -a term_type="+term_type+" -a scraping_count_goal="+str(scraping_count_goal)).split())
cmdline.execute("scrapy crawl social_keyword_spider -a start_date=2021-01-01 -a end_date=2021-06-30"
                " -a term_type=H -a scraping_count_goal=350".split())
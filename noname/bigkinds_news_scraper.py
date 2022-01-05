from datetime import datetime, date, timedelta
import subprocess
import time

import pandas as pd
import psycopg2
from scrapy import cmdline


# 지난주 월~일까지의 기간 대상으로 뉴스 스크래핑.

start_date = ""
end_date = ""
scraping_count_goal = 0

# 기간 설정
today = date.today()
start_date = today - timedelta(days=today.weekday()) - timedelta(days=7)
start_date = start_date.isoformat()
end_date = today - timedelta(days=today.weekday()) - timedelta(days=1)
end_date = end_date.isoformat()

# 하루 스크래핑 종목 개수 설정
db = psycopg2.connect(host="112.220.72.179", dbname="openmetric", user="openmetric",
                      password=")!metricAdmin01", port=2345)
cur = db.cursor()
kospi_list = pd.read_sql("select * from stocks_basic_info where corp_code!=' '", db).sort_values(
    by="code")
scraping_count_goal = int(len(kospi_list) / 7) + 20

cmdline.execute(("scrapy crawl big_kinds_news_spider "\
                "-a start_date="+start_date+" -a end_date="+end_date+" "\
                "-a scraping_count_goal="+str(scraping_count_goal)).split())
# cmdline.execute(("scrapy crawl big_kinds_news_spider "\
#                 "-a start_date=2021-09-06 -a end_date=2021-09-12 "\
#                 "-a scraping_count_goal=150").split())
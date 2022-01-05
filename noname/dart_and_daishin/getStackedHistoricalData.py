import traceback

import pandas as pd
import pandas_datareader.data as web
import io
import psycopg2
from psycopg2.extras import RealDictCursor
import pytz
from datetime import datetime, date, timedelta
import time
import yfinance as yf
import io

# from qsee_scraper.spiders import constant_var as constant
from noname.spiders import constant_var as constant

"""
새벽 즈음에 스케줄러에 의해 실행되며, 종목들에 대한 전날 주가 데이터를 수집하여 저장. 
'pandas_datareader'와 'yfinance'를 이용해 데이터를 가져오며, stocks_historic_data 테이블에 저장. 
"""

def report_error(e=None, code=""):
    date_time = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time()))
    with open(constant.error_file_path + "/dart_price_gather_error_list_" + time.strftime("%Y-%m-%d", time.localtime(
            time.time())) + ".txt", "a", encoding="UTF-8") as f:
        f.write(date_time + "_"+code+"\n")
        f.write(traceback.format_exc())

#%% get codes from db
# with psycopg2.connect(
#     user='openmetric',
#     password=')!metricAdmin01',
#     host='192.168.0.16',
#     port=5432,
#     database='openmetric'
# ) as conn:
#     cur = conn.cursor(cursor_factory=RealDictCursor)
#     query = """
#     select a.code_id as code
#     from (select count(*) cnt, code_id from stocks_historic_data where date >= '2021-08-02' and date <= '2021-08-02' group by code_id) a
#     where a.cnt = 1
#     """
#     cur.execute(query)
#     rows = cur.fetchall()
#     codes = [row['code'] for row in rows]
try:
    target_date = date.today() - timedelta(days=1)
    target_date = date.fromisoformat("2021-09-24")
    no_data_date = date.fromisoformat("2021-09-14")
    while target_date >= no_data_date:
        if target_date.weekday() in [5,6]:
            target_date = target_date - timedelta(days=1)
            continue

        start_date = target_date - timedelta(days=7)
        end_date = target_date + timedelta(days=1)

        with psycopg2.connect(
            user='openmetric',
            password=')!metricAdmin01',
            host='112.220.72.179',
            port=2345,
            database='openmetric'
        ) as conn:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            query = """
            select code from stocks_basic_info where corp_code!=' '
            """
            cur.execute(query)
            rows = cur.fetchall()
            codes = [row['code'] for row in rows]

            query="delete from stocks_historic_data where date='"+target_date.isoformat()+"'"
            cur.execute(query)


        print(len(codes))
        #%% get data from naver finance
        for code in codes:
            print(code)
            # df = web.DataReader(code[1:], 'naver', start='1990-01-02', end='2009-12-31')
            # df = web.DataReader(code[1:], 'naver', start='2010-01-02', end='2020-12-31')
            # df = web.DataReader(code[1:], 'naver', start='2021-01-02', end='2021-07-20')
            # 데이터 받을 기간 설정
            df = web.DataReader(code[1:], 'naver', start=start_date.isoformat(), end=end_date.isoformat())
            df = df[df.index == datetime.fromisoformat(target_date.isoformat())]
            if len(df) == 0: #수집하려는 날짜에 대한 데이터 없을시, 다음 종목.
                report_error(code=code)
                continue;

            df['date'] = df.index
            df['created_at'] = pytz.timezone('UTC').localize(datetime.utcnow())
            df['updated_at'] = pytz.timezone('UTC').localize(datetime.utcnow())
            df['code_id'] = code
            df['adj_close_price'] = 0.0

            csv_like = io.StringIO()
            df.to_csv(csv_like, header=False, index=False)
            csv_like.seek(0)
            cur.copy_from(
                csv_like,
                'stocks_historic_data',
                sep=',',
                columns=(
                    'open_price',
                    'high_price',
                    'low_price',
                    'close_price',
                    'transaction_volume',
                    'date',
                    'created_at',
                    'updated_at',
                    'code_id',
                    'adj_close_price'
                )
            )
            conn.commit()
            print(df)
            time.sleep(.6)

        target_date = target_date - timedelta(days=1)

except Exception as e:
    traceback.format_exc()
    report_error(e);


import traceback

import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import pytz
from datetime import datetime, date, timedelta
import time
import yfinance as yf
import io

# from qsee_scraper.spiders import constant_var as constant
from noname.spiders import constant_var as constant

# 미국주식 데이터
with psycopg2.connect(
        user='openmetric',
        password=')!metricAdmin01',
        host='112.220.72.179',
        port=2345,
        database='openmetric'
) as conn:
    cur = conn.cursor(cursor_factory=RealDictCursor)
    query = """
    select code from stocks_basic_info where currency_id = 2
    """
    cur.execute(query)
    rows = cur.fetchall()
    codes = [row['code'] for row in rows]


def report_error(e=None, code=""):
    date_time = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time()))
    with open(constant.error_file_path + "/dart_price_gather_error_list_" + time.strftime("%Y-%m-%d", time.localtime(
            time.time())) + ".txt", "a", encoding="UTF-8") as f:
        f.write(date_time + "_"+code+"\n")
        f.write(traceback.format_exc())

try:
    target_date = date.today()#- timedelta(days=1)
    start_date = target_date - timedelta(days=7)
    end_date = target_date + timedelta(days=1)

    data = dict()
    for ticker in codes:
        d = yf.download(ticker, start=start_date.isoformat(), end=end_date.isoformat())
        data[ticker] = d
        # print(d)
        time.sleep(1)

    for ticker, data in data.items():
        data['date'] = data.index
        data['created_at'] = pytz.timezone('UTC').localize(datetime.utcnow())
        data['updated_at'] = pytz.timezone('UTC').localize(datetime.utcnow())
        data['code_id'] = ticker

        csv_like = io.StringIO()
        data.to_csv(csv_like, header=False, index=False)
        csv_like.seek(0)

        with psycopg2.connect(
            user='openmetric',
            password=')!metricAdmin01',
            host='112.220.72.179',
            port=2345,
            database='openmetric'
        ) as conn:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.copy_from(
                csv_like,
                'stocks_historic_data',
                sep=',',
                columns=(
                    'open_price',
                    'high_price',
                    'low_price',
                    'close_price',
                    'adj_close_price',
                    'transaction_volume',
                    'date',
                    'created_at',
                    'updated_at',
                    'code_id'
                )
            )
            conn.commit()
            time.sleep(.6)
except Exception as e:
    traceback.format_exc()
    report_error(e)

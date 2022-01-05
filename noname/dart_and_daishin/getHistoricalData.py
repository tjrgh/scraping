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
from noname.noname.spiders import constant_var as constant

from noname import common_util
from noname.price_predict import StockPricePredictService

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

def calculate_price_predict_for_BBD():
    db = psycopg2.connect(host="112.220.72.179", dbname="openmetric", user="openmetric", password=")!metricAdmin01", port=2345)
    cur = db.cursor()

    basic_info_df = pd.read_sql("select * from stocks_basic_info where corp_code != ' ' order by code", db)
    for basic_info_idx in basic_info_df.index:
        stock_code = basic_info_df.loc[basic_info_idx]["code"]
        term = "week"

        # for i in range(1, 20):
        today = datetime.datetime.today()
        this_week_monthday = today - datetime.timedelta(days=today.weekday())
        # target_week = this_week_monthday - datetime.timedelta(days=7 * i)

        target_week_iso = common_util.datetime_to_iso(this_week_monthday)

        print("stock_code : " + stock_code)
        print("target_week : " + target_week_iso)
        print("time: " + datetime.datetime.today().isoformat())

        # 기존 데이터 존재 확인.
        # existed_predict_data = StockPricePredict.objects.filter(code_id=stock_code, date=target_week_iso)
        existed_predict_data = pd.read_sql("select * from stock_price_predict where code_id='" + stock_code + "' and date='" + target_week_iso + "'", db)
        if existed_predict_data.index.size == 7:  # 계산식의 모든 값들 저장할거면 182개.
            continue

        # 예측값 계산.
        pp = StockPricePredictService(stock_code, "week")
        pp.save_predict_data(target_week_iso)

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
    target_date = date.today()#- timedelta(days=1)
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

    calculate_price_predict_for_BBD()

    # 수정주가 가져오기
    # adjClose = pd.DataFrame()

    # 야후 파이낸스에서 수정주가 데이터  가져오기
    # for item in codes:
    #     print(f'{item}  {item[1:]}.KS')
    #     ticker = yf.Ticker(item[1:]+'.KS')
    #     # adjClose[item] = ticker.history(start='2015-01-01', end="2021-07-30", interval='1d')['Adj Close']
    #     d = yf.download(item[1:]+'.KS', start=start_date.isoformat(), end=end_date.isoformat())
    #     d = d[d.index == datetime.fromisoformat(target_date.isoformat())]
    #     adjClose[item] = d['Adj Close']
    #
    # csv_object = io.StringIO()
    # adjClose.to_csv('adj_close.csv', sep='^', header=False, index=False)
    #
    # dates = list(adjClose.index)
    # for date in dates:
    #     for c in codes:
    #         adjClose[c].fillna(0)
    #         v = adjClose[c][date]
    #         with psycopg2.connect(
    #                 user='openmetric',
    #                 password=')!metricAdmin01',
    #                 host='192.168.0.16',
    #                 port=5432,
    #                 database='openmetric'
    #         ) as conn:
    #             cur = conn.cursor()
    #             query = """
    #             update stocks_historic_data
    #             set adj_close_price = %s
    #             where code_id = %s and date = %s
    #             """
    #             cur.execute(query, (str(round(v, 2)), c, str(date).split(' ')[0]))
except Exception as e:
    traceback.format_exc()
    report_error(e);


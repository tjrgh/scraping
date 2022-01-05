import datetime

import pandas as pd
import psycopg2

import common_util
from price_predict import StockPricePredictService

db = psycopg2.connect(host="112.220.72.179", dbname="openmetric", user="openmetric", password=")!metricAdmin01", port=2345)
cur = db.cursor()

# basic_info_list = BasicInfo.objects.exclude(corp_code=" ").order_by("code").filter(code__gte='A005850').all()
basic_info_df = pd.read_sql("select * from stocks_basic_info where corp_code != ' ' and code>='A009830' order by code", db)
for basic_info_idx in basic_info_df.index:
    stock_code = basic_info_df.loc[basic_info_idx]["code"]
    term = "week"

    for i in reversed(range(1, 72)):
    # for i in range(1, 20):
        today = datetime.datetime.today()
        this_week_monthday = today - datetime.timedelta(days=today.weekday())
        target_week = this_week_monthday - datetime.timedelta(days=7 * i)

        target_week_iso = common_util.datetime_to_iso(target_week)

        print("stock_code : " + stock_code)
        print("target_week : " + target_week_iso)
        print("time: " + datetime.datetime.today().isoformat())

        # 기존 데이터 존재 확인.
        # existed_predict_data = StockPricePredict.objects.filter(code_id=stock_code, date=target_week_iso)
        existed_predict_data = pd.read_sql("select * from stock_price_predict where code_id='"+stock_code+"' and date='"+target_week_iso+"'", db)
        if existed_predict_data.index.size == 7:  # 계산식의 모든 값들 저장할거면 182개.
            continue

        # 예측값 계산.
        pp = StockPricePredictService(stock_code, "week")
        pp.save_predict_data(target_week_iso)
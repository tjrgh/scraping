import psycopg2
from psycopg2.extras import RealDictCursor
import itertools
import pandas as pd
import pytz
import io
from datetime import datetime, date, timedelta
from plusApi import get_stock_list, get_detail_data, get_history

import random
import pprint

"""
https://money2.daishin.com/e5/mboard/ptype_basic/HTS_Plus_Helper/DW_Basic_List_Page.aspx?boardseq=284 에 검색으로 
대신api 객체 설명 참고. 
"""
class CMarketTotal:
    def __init__(self):
        self.stock_info_df = pd.DataFrame()
        self.codes_in_db = []

    # 대신 api에서 받아온 종목과 db에 저장된 종목 비교하여 신규 종목을 db에 추가.
    def add_to_database(self):
        mask = ~self.stock_info_df['code'].isin(self.codes_in_db)
        new_codes = self.stock_info_df[mask]
        try:
            conn = psycopg2.connect(
                user='openmetric'
                , password=')!metricAdmin01'
                , host='192.168.0.16'
                , port='5432'
                , database='openmetric'
            )
            cursor = conn.cursor(cursor_factory=RealDictCursor)

            csv_file_like_object = io.StringIO()

            new_codes.to_csv(csv_file_like_object, header=False, index=False)
            # print(new_codes)
            csv_file_like_object.seek(0)
            cursor.copy_from(
                csv_file_like_object
                , 'stocks_basic_info'
                , sep=','
                , columns=('code', 'name', 'currency_id', 'market_id', 'created_at', 'updated_at',
                           'corp_code', 'is_recommend', 'is_favorite')
            )
            conn.commit()
        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL", error)
        finally:
            if conn:
                conn.close()

    def get_all_market_stocks(self):
        market_list = ['코스피', '코스닥']
        for idx, value in enumerate(market_list):
            stock_info = get_stock_list(value)# 종목들에 대한 여러 데이터가 들어있는 dict 객체.
            # print(len(stock_info.keys()))
            df = pd.DataFrame(list(stock_info.items()), columns=['code', 'name'])
            df['currency_id'] = 1
            df['market_id'] = idx + 1
            df['created_at'] = pytz.timezone('UTC').localize(datetime.utcnow())
            df['updated_at'] = pytz.timezone('UTC').localize(datetime.utcnow())
            df['corp_code'] = ' '
            df['is_recommend'] = False
            df['is_favorite'] = False
            print(len(df.index))
            if idx == 0: # self.stock_info_df에 종목 데이터 리스트 연결.
                self.stock_info_df = df
            else:
                self.stock_info_df = pd.concat([self.stock_info_df, df], ignore_index=True)
        # print(self.stock_info_df)
        self.get_codes_from_db()
        self.add_to_database()

    # self.codes_in_db 변수에 code목록 저장.
    def get_codes_from_db(self):
        try:
            conn = psycopg2.connect(
                user='openmetric'
                , password=')!metricAdmin01'
                , host='192.168.0.16'
                , port='5432'
                , database='openmetric'
            )
            cursor = conn.cursor()
            sql = '''
            select code from stocks_basic_info
            '''
            cursor.execute(sql)
            self.codes_in_db = [r[0] for r in cursor.fetchall()]
        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL", error)
        finally:
            if conn:
                conn.close()

    # db의 종목 리스트에 대한 여러 데이터들을 대신api로부터 받아와 stock_basic_info 테이블에 db에 저장.
    def get_detail_info(self):
        self.get_codes_from_db()
        try:
            conn = psycopg2.connect(
                user='openmetric'
                , password=')!metricAdmin01'
                , host='192.168.0.16'
                , port='5432'
                , database='openmetric'
            )
            cursor = conn.cursor()

            data = list(get_detail_data(self.codes_in_db).values())# 종목 리스트에 대한 여러 데이터가 들은 dict객체.

            data_df = pd.DataFrame.from_records(data)
            data_df['created_at'] = pytz.timezone('UTC').localize(datetime.utcnow())
            data_df['updated_at'] = pytz.timezone('UTC').localize(datetime.utcnow())
            data_df['data_date'] = date.today()

            data_df[2] = [int(chr(x)) for x in data_df[2].tolist()]
            data_df[12] = [4 if x == 0 else int(chr(x))+1 for x in data_df[12].tolist()]
            data_df[22] = [3 if x == 0 else int(chr(x)) for x in data_df[22].tolist()]
            data_df[26] = [6 if x == 0 else int(chr(x)) for x in data_df[26].tolist()]
            data_df[63] = [100001 if x == '' else x for x in data_df[63].tolist()]
            data_df[79] = [100001 if x == '' else x for x in data_df[79].tolist()]
            data_df[81] = [100001 if x == '' else x for x in data_df[81].tolist()]
            data_df[85] = [1 if x == 0 else int(chr(x))+1 for x in data_df[85].tolist()]
            data_df[87] = [1 if x == 0 else int(chr(x))+1 for x in data_df[87].tolist()]
            data_df[101] = [1 if x == 0 else int(chr(x))+1 for x in data_df[101].tolist()]

            csv_file_like_object = io.StringIO()
            data_df.to_csv(csv_file_like_object, header=False, index=False)

            csv_file_like_object.seek(0)
            cursor.copy_from(
                csv_file_like_object
                , 'stocks_detail_info'
                , sep=','
                , columns=(
                    'code_id',
                    'time',
                    'updown_signal',
                    'updown_price',
                    'current_price',
                    'open_price',
                    'high_price',
                    'low_price',
                    'ask_quote',
                    'bid_quote',
                    'transaction_volume',
                    'transaction_price',
                    'market_state',
                    'total_ask_quote_redundancy',
                    'total_bid_quote_redundancy',
                    'first_ask_quote_redundancy',
                    'first_bid_quote_redundancy',
                    'total_listing_volume',
                    'foreigner_holding_ration',
                    'previous_volume',
                    'previous_close_price',
                    'volume_power',
                    'volume_type',
                    'open_interest',
                    'expected_closing_price',
                    'expected_closing_updown',
                    'expected_closing_updown_signal',
                    'expected_volume',
                    'nineteen_closing_sum',
                    'upper_limit',
                    'lower_limit',
                    'sales_quantity_unit',
                    'foreigner_net_sale_volume',
                    'fiftytwoweek_high_price',
                    'fiftytwoweek_low_price',
                    'year_high_price',
                    'year_low_price',
                    'price_earning_ratio',
                    'earning_per_share',
                    'capital',
                    'par_value',
                    'allocation_ratio',
                    'allocation_earning_ratio',
                    'debt_ratio',
                    'reservation_ratio',
                    'equity_capital_ratio',
                    'sales_growth_ratio',
                    'ordinary_profit_growth_ratio',
                    'net_profit_growth_ratio',
                    'sentiment_indicators',
                    'volume_ratio',
                    'fiveday_turnover_ratio',
                    'fourday_closeprice_sum',
                    'nineday_closeprice_sum',
                    'revenue',
                    'ordinary_profit',
                    'net_profit',
                    'bookvalue_per_share',
                    'operating_income_growth_ratio',
                    'operating_income',
                    'operating_income_to_sales_ratio',
                    'ordinary_profit_to_sales_ratio',
                    'interest_coverage_ratio',
                    'closing_account_date',
                    'quarter_bookvalue_per_share',
                    'quarter_revenue_growth_ratio',
                    'quarter_operating_income_growth_ratio',
                    'quarter_ordinary_profit_growth_ratio',
                    'quarter_net_profit_growth_ratio',
                    'quarter_sales',
                    'quarter_operating_income',
                    'quarter_ordinary_profit',
                    'quarter_net_profit',
                    'quarter_operating_income_to_sales_ratio',
                    'quarter_ordinary_profit_to_sales_ratio',
                    'quarter_return_on_equity',
                    'quarter_interest_coverage_ratio',
                    'quarter_reserve_ratio',
                    'quarter_debr_ration',
                    'last_quarter_yyyymm',
                    'basis',
                    'local_date_yyyymmdd',
                    'nation',
                    'elw_theoretical_value',
                    'program_net_bid',
                    'today_foregier_net_bid_porvisional_yesno',
                    'today_foregier_net_bid',
                    'today_institution_net_bid_porvisional_yesno',
                    'today_institution_net_bid',
                    'previous_foregier_net_bid',
                    'previous_institution_net_bid',
                    'sales_per_share',
                    'cash_flow_per_share',
                    'earning_before_interest_tax_depreciation_amortization',
                    'credit_balance_ratio',
                    'short_selling_quantity',
                    'short_selling_date',
                    'index_futures_previous_unpaid_agreement',
                    'beta',
                    'fiftynine_close_sum',
                    'oneonenine_close_sum',
                    'today_retail_net_bid_porvisional_yesno',
                    'today_retail_net_bid',
                    'previous_retail_net_bid',
                    'five_previous_close_price',
                    'ten_previous_close_price',
                    'twenty_previous_close_price',
                    'sixty_previous_close_price',
                    'onehundredtwenty_previous_close_price',
                    'estimated_static_vi_activation_base_price',
                    'estimated_static_vi_activation_rising_price',
                    'estimated_static_vi_activation_falling_price',
                    'created_at',
                    'updated_at',
                    'data_date'
                )
            )

            conn.commit()
        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL", error)
        finally:
            if conn:
                conn.close()

    # 종목에 대한 점수 테이블(stocks_parameter)에 랜덤값 점수 부여 함수.
    def rand_num(self):
        codes = None
        try:
            conn = psycopg2.connect(
                user='openmetric'
                , password=')!metricAdmin01'
                , host='192.168.0.16'
                , port='5432'
                , database='openmetric'
            )
            cursor = conn.cursor()
            sql = '''
            select code from stocks_basic_info
            '''
            cursor.execute(sql)
            codes = [r[0] for r in cursor.fetchall()]

            data = []
            for code in codes:
                attractive = round(random.uniform(0, 10), 2)
                growth = round(random.uniform(0, 10), 2)
                stability = round(random.uniform(0, 10), 2)
                cash_generate = round(random.uniform(0, 10), 2)
                monopoly = round(random.uniform(0, 10), 2)
                recommendation_value = round((attractive + growth + stability + cash_generate + monopoly) * 2)
                d = {
                    'code_id': code,
                    'attractive': attractive,
                    'growth': growth,
                    'stability': stability,
                    'cash_generate':  cash_generate,
                    'monopoly': monopoly,
                    'recommendation_value': recommendation_value,
                }
                data.append(d)

            data_df = pd.DataFrame.from_records(data)
            data_df['created_at'] = pytz.timezone('UTC').localize(datetime.utcnow())
            data_df['updated_at'] = pytz.timezone('UTC').localize(datetime.utcnow())
            data_df['data_date'] = date.today()

            csv_file_like_object = io.StringIO()
            data_df.to_csv(csv_file_like_object, header=False, index=False)

            csv_file_like_object.seek(0)
            cursor.copy_from(
                csv_file_like_object
                , 'stocks_parameter'
                , sep=','
                , columns=(
                    'code_id',
                    'attractive',
                    'growth',
                    'stability',
                    'cash_generate',
                    'monopoly',
                    'recommendation_value',
                    'created_at',
                    'updated_at',
                    'data_date'
                )
            )

            conn.commit()
        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL", error)
        finally:
            if conn:
                conn.close()

    # ㅅ
    def get_history_data(self):
        try:
            conn = psycopg2.connect(
                user='openmetric'
                , password=')!metricAdmin01'
                , host='192.168.0.16'
                , port='5432'
                , database='openmetric'
            )
            cursor = conn.cursor()
            sql = '''
            select code from stocks_basic_info
            '''
            cursor.execute(sql)
            codes = [r[0] for r in cursor.fetchall()]
            data, dates = get_history(codes)
            df = pd.DataFrame(data, dates)
            print(df)
        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL", error)
        finally:
            if conn:
                conn.close()



if __name__ == '__main__':
    market_total = CMarketTotal()
    market_total.get_all_market_stocks()
    # market_total.get_detail_info()
    # market_total.rand_num()
    # market_total.get_history_data()

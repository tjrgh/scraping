import datetime
import traceback

import pandas as pd
import psycopg2
from statsmodels.tsa.arima.model import ARIMA

# from ubold.stocks.models import FinancialStatement, HistoricData, StockPricePredict, BasicInfo
import common_util

class StockPricePredictService():

    stock_code = ""
    price_list = None
    predict_price_list = None
    term = "week"

    n_list = [3,5,10,20]

    account = {
        "S": None,
        "H": None,
        "L": None,
        "E": None,
        "G": None,
        "Gp": None,
        "HLk": None,
        "HLk_n_d": n_list.copy(),
        "HLpk": None,
        "HLpk_n_d": None,
        "HSk": None,
        "HSk_n_d": n_list.copy(),
        "SLk": None,
        "SLk_n_d": n_list.copy(),
        "n_EHP1": n_list.copy(),
        "n_ELP1": n_list.copy(),
        "n_HLW1": n_list.copy(),
        "n_HLMA1": n_list.copy(),
        "n_HPEE1": n_list.copy(),
        "n_LPEE1": n_list.copy(),
        "3HPEEA1": None,
        "3LPEEA1": None,

        "n_EHP2": n_list.copy(),
        "n_ELP2": n_list.copy(),
        "n_HLW2": n_list.copy(),
        "n_HLMA2": n_list.copy(),
        "n_HPEE2": n_list.copy(),
        "n_LPEE2": n_list.copy(),
        "3HPEEA2": None,
        "3LPEEA2": None,

        "n_EHP3": n_list.copy(),
        "n_ELP3": n_list.copy(),
        "n_HLW3": n_list.copy(),
        "n_HLMA3": n_list.copy(),
        "n_HPEE3": n_list.copy(),
        "n_LPEE3": n_list.copy(),
        "3HPEEA3": None,
        "3LPEEA3": None,

        "n_EHP123": n_list.copy(),
        "n_ELP123": n_list.copy(),
        "n_HLW123": n_list.copy(),
        "n_HLMA123": n_list.copy(),
        "n_HPEE123": n_list.copy(),
        "n_LPEE123": n_list.copy(),
        "3HPEEA123": None,
        "3LPEEA123": None,

    }

    def __init__(self, stock_code, term):
        self.db = psycopg2.connect(host="112.220.72.179", dbname="openmetric", user="openmetric", password=")!metricAdmin01", port=2345)
        self.cur = self.db.cursor()

        self.stock_code = stock_code
        # self.price_list = HistoricData.objects.filter(code=stock_code).all()
        self.price_list = pd.read_sql("select * from stocks_historic_data where code_id='"+self.stock_code+"'", self.db)

        # self.predict_price_list = StockPricePredict.objects.filter(code=stock_code).all()
        self.predict_price_list = pd.read_sql("select * from stock_price_predict where code_id='"+self.stock_code+"'", self.db)
        self.term = term

    def insert_to_db_table_from_model(self, stock_price_predict_model, basic_info, ):
        date = stock_price_predict_model.date
        account_name = stock_price_predict_model.account_name
        account_code =stock_price_predict_model.account_code
        account_value = stock_price_predict_model.account_value

        db = psycopg2.connect(host="112.220.72.179", dbname="openmetric", user="openmetric", password=")!metricAdmin01", port=2345)
        cur = db.cursor()

        try:
            cur.execute("INSERT INTO stock_price_predict ("
                        "created_at, updated_at, code_id, date, account_name, account_code, account_value"
                        ") "
                        "VALUES ( "
                        "   '" + str(datetime.datetime.now(datetime.timezone.utc)) + "', '" + str(datetime.datetime.now(datetime.timezone.utc)) + "', "
                        "   '" + basic_info.code + "', "+"'"+date+"', "+"'"+account_name+"', "+"'"+account_code+"', "+"'"+str(account_value)+"'"
                        ")"
                        )
            db.commit()
        except:
            traceback.format_exc()
    def insert_to_db_table_from_model2(self, date, account_name, account_code, account_value, ):
        try:
            self.cur.execute("INSERT INTO stock_price_predict ("
                        "created_at, updated_at, code_id, date, account_name, account_code, account_value"
                        ") "
                        "VALUES ( "
                        "   '" + str(datetime.datetime.now(datetime.timezone.utc)) + "', '" + str(datetime.datetime.now(datetime.timezone.utc)) + "', "
                        "   '" + self.stock_code + "', "+"'"+date+"', "+"'"+account_name+"', "+"'"+account_code+"', "+"'"+str(account_value)+"'"
                        ")"
                        )
            self.db.commit()
        except:
            traceback.format_exc()


    # 날짜에 대해 예상값들 계산하여 저장.
    # @transaction.commit_manually
    def save_predict_data(self, k):
        print("initialize data start")

        # 거래데이터가 모두 정상적으로 db에 쌓여있다는 보장.
        # !!!!! 이를 표시해주는 db 컬럼을 추가하거나, 안 좋긴 하지만 거래 데이터 스크래핑 바로 다음에 계산식 계산이 수행될 수 있도록 함으로서 해결??

        # 기존값 삭제
        self.cur.execute("delete from stock_price_predict where date='"+k+"' and code_id='"+self.stock_code+"'")
        self.db.commit()

        S = self.S(k)
        # self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code="S", account_name="시가", account_value=S), basicInfo)
        self.insert_to_db_table_from_model2(k, "시가", "S", S)
        H = self.H(k)
        # self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code="H", account_name="고가", account_value=H), basicInfo)
        self.insert_to_db_table_from_model2(k, "고가", "H", H)
        L = self.L(k)
        # self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code="L", account_name="저가", account_value=L), basicInfo)
        self.insert_to_db_table_from_model2(k, "저가", "L", L)
        E = self.E(k)
        # self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code="E", account_name="종가", account_value=E), basicInfo)
        self.insert_to_db_table_from_model2(k, "종가", "E", E)

        _3EHP123 = self.n_EHP123(k, 3)
        # self.insert_to_db_table_from_model(
        #     StockPricePredict(code=basicInfo, date=k, account_code="3EHP123", account_name="123차 예상고저가 - 3일 예상고가", account_value=_3EHP123), basicInfo)
        self.insert_to_db_table_from_model2(k, "123차 예상고저가 - 3일 예상고가", "3EHP123", _3EHP123)
        _3ELP123 = self.n_ELP123(k, 3)
        # self.insert_to_db_table_from_model(
        #     StockPricePredict(code=basicInfo, date=k, account_code="3ELP123", account_name="123차 예상고저가 - 3일 예상저가", account_value=_3ELP123), basicInfo)
        self.insert_to_db_table_from_model2(k, "123차 예상고저가 - 3일 예상저가", "3ELP123", _3ELP123)
        arima_E_predict = self.arima_E_predict(k)
        if arima_E_predict != None:
            # self.insert_to_db_table_from_model(
            #     StockPricePredict(code=basicInfo, date=k, account_code="arima_E", account_name="ARIMA 회귀 예측 종가", account_value=arima_E_predict), basicInfo
            # )
            self.insert_to_db_table_from_model2(k, "ARIMA 회귀 예측 종가", "arima_E", arima_E_predict)


    # def arima_E_predict2(self,k):
    #     stock_code = self.stock_code
    #     target_date_datetime = common_util.iso_to_datetime(k)
    #     today_day = target_date_datetime.weekday()
    #     if today_day >= 4:  # 금요일 이후이면,
    #         start_date = target_date_datetime - datetime.timedelta(days=today_day) - datetime.timedelta(weeks=19)
    #         end_date = target_date_datetime - datetime.timedelta(days=today_day)
    #     else:
    #         start_date = target_date_datetime - datetime.timedelta(days=today_day) - datetime.timedelta(weeks=19)
    #         end_date = target_date_datetime - datetime.timedelta(days=today_day) - datetime.timedelta(weeks=0)
    #     start_date_iso = common_util.datetime_to_iso(start_date)
    #     end_date_iso = common_util.datetime_to_iso(end_date)
    #
    #     stock_price_df = pd.DataFrame(
    #         list(StockPricePredict.objects.filter(code_id=stock_code, date__gte=start_date_iso, date__lte=end_date_iso,
    #                                               account_code__in=["S", "E", "H", "L", "3EHP123", "3ELP123"]).order_by(
    #             "date").values()))
    #     # 프론트에서 필요한 형식에 맞게 데이터 가공.
    #     e_df = stock_price_df[stock_price_df["account_code"] == "E"]
    #
    #     def convertToInt(x):
    #         return int(x)
    #
    #     data = pd.concat([e_df['date'], e_df['account_value']], axis=1, keys=['date', 'price'])
    #     data['date'] = pd.to_datetime(data.date, format='%Y-%m-%d')
    #     data['price'] = pd.to_numeric(data.price)
    #     data = data.set_index('date')
    #
    #     model = ARIMA(endog=data['price'], order=(1, 1, 1))
    #     model_fit = model.fit()
    #     # print(model_fit.summary())
    #     output_f = model_fit.forecast()
    #     print(f'date: {output_f.index[0]}, predict_price: {output_f[0]}')
    #     return output_f[0]

    def arima_E_predict(self, k):
        stock_code = self.stock_code
        target_date_datetime = common_util.iso_to_datetime(k)
        target_date_day = common_util.iso_to_datetime(k).weekday()
        # if target_date_day >= 4:  # 금요일 이후이면,
        #     start_date = target_date_datetime - datetime.timedelta(days=target_date_day) - datetime.timedelta(weeks=19)
        #     end_date = target_date_datetime - datetime.timedelta(days=target_date_day)
        # else:
        #     start_date = target_date_datetime - datetime.timedelta(days=target_date_day) - datetime.timedelta(weeks=20)
        #     end_date = target_date_datetime - datetime.timedelta(days=target_date_day) - datetime.timedelta(weeks=1)

        start_date = target_date_datetime - datetime.timedelta(days=target_date_day) - datetime.timedelta(weeks=19)

        start_date_iso = common_util.datetime_to_iso(start_date)
        # end_date_iso = common_util.datetime_to_iso(end_date)

        # e_df = pd.DataFrame(
        #     list(StockPricePredict.objects.filter(code_id=stock_code, date__gte=start_date_iso, date__lte=k,
        #                                           account_code__in=["E"]).order_by("date").values()))
        e_df = pd.read_sql("select * from stock_price_predict "
                            "where code_id='"+self.stock_code+"' and date>='"+start_date_iso+"' and date<='"+k+"' and account_code='E' "
                            "order by date", self.db)
        if len(e_df.index) < 20:
            return

        data = pd.concat([e_df['date'], e_df['account_value']], axis=1, keys=['date', 'price'])
        data['date'] = pd.to_datetime(data.date, format='%Y-%m-%d')
        data['price'] = pd.to_numeric(data.price)
        data = data.set_index('date')

        model = ARIMA(endog=data['price'], order=(1, 1, 1))
        model_fit = model.fit()
        # print(model_fit.summary())
        output_f = model_fit.forecast()
        print(f'date: {output_f.index[0]}, predict_price: {output_f[0]}')
        return output_f[0]

        # G = self.G(k)
        # self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code="G", account_name="갭등락폭", account_value=G), basicInfo)
        # Gp = self.Gp(k)
        # self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code="Gp", account_name="갭등락률", account_value=Gp), basicInfo)
        # HLk= self.HLk(k)
        # self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code="HLk", account_name="당일고저변동폭", account_value=HLk), basicInfo)
        # HLk3d = self.HLk3d(k)
        # self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code="HLk3d", account_name="당일고저변동폭 전3일 이평", account_value=HLk3d), basicInfo)
        # HLk5d = self.HLk5d(k)
        # self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code="HLk5d", account_name="당일고저변동폭 전5일 이평", account_value=HLk5d), basicInfo)
        # HLk10d = self.HLk10d(k)
        # self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code="HLk10d", account_name="당일고저변동폭 전10일 이평", account_value=HLk10d), basicInfo)
        # HLk20d = self.HLk20d(k)
        # self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code="HLk20d", account_name="당일고저변동폭 전20일 이평", account_value=HLk20d), basicInfo)
        #
        # HLpk =self.HLpk(k)
        # self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code="HLpk", account_name="당일고저변동률", account_value=HLpk), basicInfo)
        # HLpk3d = self.HLpk3d(k)
        # self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code="HLpk3d", account_name="당일고저변동률 전3일 이평", account_value=HLpk3d), basicInfo)
        # HLpk5d = self.HLpk5d(k)
        # self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code="HLpk5d", account_name="당일고저변동률 전5일 이평", account_value=HLpk5d), basicInfo)
        # HLpk10d = self.HLpk10d(k)
        # self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code="HLpk10d", account_name="당일고저변동률 전10일 이평", account_value=HLpk10d), basicInfo)
        # HLpk20d = self.HLpk20d(k)
        # self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code="HLpk20d", account_name="당일고저변동률 전20일 이평", account_value=HLpk20d), basicInfo)
        #
        # HSk = self.HSk(k)
        # self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code="HSk", account_name="고가 시가 변동폭", account_value=HSk), basicInfo)
        # HSk_n_d = []
        # for i in self.n_list:
        #     HSk_n_d.append(self.HSk_n_d(k, i))
        # for i in range(len(self.n_list)):
        #     n = str(self.n_list[i])
        #     self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code="HSk" + n + "d", account_name="고가시가 변동폭 전" + n + "일 이평", account_value=HSk_n_d[i]), basicInfo)
        #
        # SLk = self.SLk(k)
        # self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code="SLk", account_name="시가 저가 변동폭", account_value=SLk), basicInfo)
        #
        # SLk_n_d = []
        # for i in self.n_list:
        #     SLk_n_d.append(self.SLk_n_d(k, i))
        # for i in range(len(self.n_list)):
        #     n = str(self.n_list[i])
        #     self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code="SLk" + n + "d", account_name="시가 저가 변동폭 전" + n + "일 이평", account_value=SLk_n_d[i]), basicInfo)
        #
        #
        # print("1차 시작")
        # # 1차
        # n_EHP1 = []
        # for i in self.n_list:
        #     n_EHP1.append(self.n_EHP1(k, i))
        # for i in range(len(self.n_list)):
        #     n = str(self.n_list[i])
        #     self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code=n + "EHP1", account_name="1차 예상고저가 - " + n + "일 예상고가", account_value=n_EHP1[i]), basicInfo)
        #
        # n_ELP1 = []
        # for i in self.n_list:
        #     n_ELP1.append(self.n_ELP1(k, i))
        # for i in range(len(self.n_list)):
        #     n = str(self.n_list[i])
        #     self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code=n + "ELP1", account_name="1차 예상고저가 - " + n + "일 예상저가", account_value=n_ELP1[i]), basicInfo)
        #
        # n_HLW1 = []
        # for i in self.n_list:
        #     n_HLW1.append(self.n_HLW1(k, i))
        # for i in range(len(self.n_list)):
        #     n = str(self.n_list[i])
        #     self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code=n + "HLW1", account_name="1차 예상고저가 - 고저변동폭 " + n + "일", account_value=n_HLW1[i]), basicInfo)
        #
        # n_HLMA1 = []
        # for i in self.n_list:
        #     n_HLMA1.append(self.n_HLMA1(k, i))
        # for i in range(len(self.n_list)):
        #     n = str(self.n_list[i])
        #     self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code=n + "HLMA1", account_name="1차 예상고저가 - 전5일 이평 " + n + "일", account_value=n_HLMA1[i]), basicInfo)
        #
        # n_HPEE1 = []
        # for i in self.n_list:
        #     n_HPEE1.append(self.n_HPEE1(k, i))
        # for i in range(len(self.n_list)):
        #     n = str(self.n_list[i])
        #     self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code=n + "HPEE1", account_name="1차 예측오차 - " + n + "일 고가예측오차", account_value=n_HPEE1[i]), basicInfo)
        #
        # n_LPEE1 = []
        # for i in self.n_list:
        #     n_LPEE1.append(self.n_LPEE1(k, i))
        # for i in range(len(self.n_list)):
        #     n = str(self.n_list[i])
        #     self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code=n + "LPEE1", account_name="1차 예측오차 - " + n + "일 저가예측오차", account_value=n_LPEE1[i]), basicInfo)
        #
        # _3HPEEA1 = self._3HPEEA1(k)
        # _3LPEEA1 = self._3LPEEA1(k)
        # self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code="3HPEEA1", account_name="고가예측 오차평균(3일)", account_value=_3HPEEA1), basicInfo)
        # self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code="3LPEEA1", account_name="저가예측 오차평균(3일)", account_value=_3LPEEA1), basicInfo)
        #
        # print("2차 시작")
        # # 2차
        # n_EHP2 = []
        # for i in self.n_list:
        #     n_EHP2.append(self.n_EHP2(k, i))
        # for i in range(len(self.n_list)):
        #     n = str(self.n_list[i])
        #     self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code=n + "EHP2", account_name="2차 예상고저가 - " + n + "일 예상고가", account_value=n_EHP2[i]), basicInfo)
        #
        # n_ELP2 = []
        # for i in self.n_list:
        #     n_ELP2.append(self.n_ELP2(k, i))
        # for i in range(len(self.n_list)):
        #     n = str(self.n_list[i])
        #     self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code=n + "ELP2", account_name="2차 예상고저가 - " + n + "일 예상저가", account_value=n_ELP2[i]), basicInfo)
        #
        # n_HLW2 = []
        # for i in self.n_list:
        #     n_HLW2.append(self.n_HLW2(k, i))
        # for i in range(len(self.n_list)):
        #     n = str(self.n_list[i])
        #     self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code=n + "HLW2", account_name="2차 예상고저가 - 고저변동폭 " + n + "일", account_value=n_HLW2[i]), basicInfo)
        #
        # n_HLMA2 = []
        # for i in self.n_list:
        #     n_HLMA2.append(self.n_HLMA2(k, i))
        # for i in range(len(self.n_list)):
        #     n = str(self.n_list[i])
        #     self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code=n + "HLMA2", account_name="2차 예상고저가 - 전5일 이평 " + n + "일", account_value=n_HLMA2[i]), basicInfo)
        #
        # n_HPEE2 = []
        # for i in self.n_list:
        #     n_HPEE2.append(self.n_HPEE2(k, i))
        # for i in range(len(self.n_list)):
        #     n = str(self.n_list[i])
        #     self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code=n + "HPEE2", account_name="2차 예측오차 - " + n + "일 고가예측오차", account_value=n_HPEE2[i]), basicInfo)
        #
        # n_LPEE2 = []
        # for i in self.n_list:
        #     n_LPEE2.append(self.n_LPEE2(k, i))
        # for i in range(len(self.n_list)):
        #     n = str(self.n_list[i])
        #     self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code=n + "LPEE2", account_name="2차 예측오차 - " + n + "일 저가예측오차", account_value=n_LPEE2[i]), basicInfo)
        #
        # _3HPEEA2 = self._3HPEEA2(k)
        # _3LPEEA2 = self._3LPEEA2(k)
        # self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code="3HPEEA2", account_name="고가예측 오차평균(3일)", account_value=_3HPEEA2), basicInfo)
        # self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code="3LPEEA2", account_name="저가예측 오차평균(3일)", account_value=_3LPEEA2), basicInfo)
        #
        # print("3차 시작")
        # # 3차
        # n_EHP3 = []
        # for i in self.n_list:
        #     n_EHP3.append(self.n_EHP3(k, i))
        # for i in range(len(self.n_list)):
        #     n = str(self.n_list[i])
        #     self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code=n + "EHP3", account_name="3차 예상고저가 - " + n + "일 예상고가", account_value=n_EHP3[i]), basicInfo)
        #
        # n_ELP3 = []
        # for i in self.n_list:
        #     n_ELP3.append(self.n_ELP3(k, i))
        # for i in range(len(self.n_list)):
        #     n = str(self.n_list[i])
        #     self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code=n + "ELP3", account_name="3차 예상고저가 - " + n + "일 예상저가", account_value=n_ELP3[i]), basicInfo)
        #
        # n_HLW3 = []
        # for i in self.n_list:
        #     n_HLW3.append(self.n_HLW3(k, i))
        # for i in range(len(self.n_list)):
        #     n = str(self.n_list[i])
        #     self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code=n + "HLW3", account_name="3차 예상고저가 - 고저변동폭 " + n + "일", account_value=n_HLW3[i]), basicInfo)
        #
        # n_HLMA3 = []
        # for i in self.n_list:
        #     n_HLMA3.append(self.n_HLMA3(k, i))
        # for i in range(len(self.n_list)):
        #     n = str(self.n_list[i])
        #     self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code=n + "HLMA3", account_name="3차 예상고저가 - 전5일 이평 " + n + "일", account_value=n_HLMA3[i]), basicInfo)
        #
        # n_HPEE3 = []
        # for i in self.n_list:
        #     n_HPEE3.append(self.n_HPEE3(k, i))
        # for i in range(len(self.n_list)):
        #     n = str(self.n_list[i])
        #     self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code=n + "HPEE3", account_name="3차 예측오차 - " + n + "일 고가예측오차", account_value=n_HPEE3[i]), basicInfo)
        #
        # n_LPEE3 = []
        # for i in self.n_list:
        #     n_LPEE3.append(self.n_LPEE3(k, i))
        # for i in range(len(self.n_list)):
        #     n = str(self.n_list[i])
        #     self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code=n + "LPEE3", account_name="3차 예측오차 - " + n + "일 저가예측오차", account_value=n_LPEE3[i]), basicInfo)
        #
        # _3HPEEA3 = self._3HPEEA3(k)
        # _3LPEEA3 = self._3LPEEA3(k)
        # self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code="3HPEEA3", account_name="고가예측 오차평균(3일)", account_value=_3HPEEA3), basicInfo)
        # self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code="3LPEEA3", account_name="저가예측 오차평균(3일)", account_value=_3LPEEA3), basicInfo)
        #
        # print("123차 시작")
        # # 123차
        # n_EHP123 = []
        # for i in self.n_list:
        #     n_EHP123.append(self.n_EHP123(k, i))
        # for i in range(len(self.n_list)):
        #     n = str(self.n_list[i])
        #     self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code=n + "EHP123", account_name="123차 예상고저가 - " + n + "일 예상고가", account_value=n_EHP123[i]), basicInfo)
        #
        # n_ELP123 = []
        # for i in self.n_list:
        #     n_ELP123.append(self.n_ELP123(k, i))
        # for i in range(len(self.n_list)):
        #     n = str(self.n_list[i])
        #     self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code=n + "ELP123", account_name="123차 예상고저가 - " + n + "일 예상저가", account_value=n_ELP123[i]), basicInfo)
        #
        # n_HLW123 = []
        # for i in self.n_list:
        #     n_HLW123.append(self.n_HLW123(k, i))
        # for i in range(len(self.n_list)):
        #     n = str(self.n_list[i])
        #     self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code=n + "HLW123", account_name="123차 예상고저가 - 고저변동폭 " + n + "일", account_value=n_HLW123[i]), basicInfo)
        #
        # n_HLMA123 = []
        # for i in self.n_list:
        #     n_HLMA123.append(self.n_HLMA123(k, i))
        # for i in range(len(self.n_list)):
        #     n = str(self.n_list[i])
        #     self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code=n + "HLMA123", account_name="123차 예상고저가 - 전5일 이평 " + n + "일", account_value=n_HLMA123[i]), basicInfo)
        #
        # n_HPEE123 = []
        # for i in self.n_list:
        #     n_HPEE123.append(self.n_HPEE123(k, i))
        # for i in range(len(self.n_list)):
        #     n = str(self.n_list[i])
        #     self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code=n + "HPEE123", account_name="123차 예측오차 - " + n + "일 고가예측오차", account_value=n_HPEE123[i]), basicInfo)
        #
        # n_LPEE123 = []
        # for i in self.n_list:
        #     n_LPEE123.append(self.n_LPEE123(k, i))
        # for i in range(len(self.n_list)):
        #     n = str(self.n_list[i])
        #     self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code=n + "LPEE123", account_name="123차 예측오차 - " + n + "일 저가예측오차", account_value=n_LPEE123[i]), basicInfo)
        #
        # _3HPEEA123 = self._3HPEEA123(k)
        # _3LPEEA123 = self._3LPEEA123(k)
        # self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code="3HPEEA123", account_name="고가예측 오차평균(3일)", account_value=_3HPEEA123), basicInfo)
        # self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code="3LPEEA123", account_name="저가예측 오차평균(3일)", account_value=_3LPEEA123), basicInfo)
        #
        # print("12차 시작")
        # # 12차
        # n_EHP12 = []
        # for i in self.n_list:
        #     n_EHP12.append(self.n_EHP12(k, i))
        # for i in range(len(self.n_list)):
        #     n = str(self.n_list[i])
        #     self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code=n + "EHP12", account_name="12차 예상고저가 - " + n + "일 예상고가", account_value=n_EHP12[i]), basicInfo)
        #
        # n_ELP12 = []
        # for i in self.n_list:
        #     n_ELP12.append(self.n_ELP12(k, i))
        # for i in range(len(self.n_list)):
        #     n = str(self.n_list[i])
        #     self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code=n + "ELP12", account_name="12차 예상고저가 - " + n + "일 예상저가", account_value=n_ELP12[i]), basicInfo)
        #
        # n_HLW12 = []
        # for i in self.n_list:
        #     n_HLW12.append(self.n_HLW12(k, i))
        # for i in range(len(self.n_list)):
        #     n = str(self.n_list[i])
        #     self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code=n + "HLW12", account_name="12차 예상고저가 - 고저변동폭 " + n + "일", account_value=n_HLW12[i]), basicInfo)
        #
        # n_HLMA12 = []
        # for i in self.n_list:
        #     n_HLMA12.append(self.n_HLMA12(k, i))
        # for i in range(len(self.n_list)):
        #     n = str(self.n_list[i])
        #     self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code=n + "HLMA12", account_name="12차 예상고저가 - 전5일 이평 " + n + "일", account_value=n_HLMA12[i]), basicInfo)
        #
        # n_HPEE12 = []
        # for i in self.n_list:
        #     n_HPEE12.append(self.n_HPEE12(k, i))
        # for i in range(len(self.n_list)):
        #     n = str(self.n_list[i])
        #     self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code=n + "HPEE12", account_name="12차 예측오차 - " + n + "일 고가예측오차", account_value=n_HPEE12[i]), basicInfo)
        #
        # n_LPEE12 = []
        # for i in self.n_list:
        #     n_LPEE12.append(self.n_LPEE12(k, i))
        # for i in range(len(self.n_list)):
        #     n = str(self.n_list[i])
        #     self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code=n + "LPEE12", account_name="12차 예측오차 - " + n + "일 저가예측오차", account_value=n_LPEE12[i]), basicInfo)
        #
        # _3HPEEA12 = self._3HPEEA12(k)
        # _3LPEEA12 = self._3LPEEA12(k)
        # self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code="3HPEEA12", account_name="고가예측 오차평균(3일)", account_value=_3HPEEA12), basicInfo)
        # self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code="3LPEEA12", account_name="저가예측 오차평균(3일)", account_value=_3LPEEA12), basicInfo)
        #
        # print("23차 시작")
        # # 23차
        # n_EHP23 = []
        # for i in self.n_list:
        #     n_EHP23.append(self.n_EHP23(k, i))
        # for i in range(len(self.n_list)):
        #     n = str(self.n_list[i])
        #     self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code=n + "EHP23", account_name="23차 예상고저가 - " + n + "일 예상고가", account_value=n_EHP23[i]), basicInfo)
        #
        # n_ELP23 = []
        # for i in self.n_list:
        #     n_ELP23.append(self.n_ELP23(k, i))
        # for i in range(len(self.n_list)):
        #     n = str(self.n_list[i])
        #     self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code=n + "ELP23", account_name="23차 예상고저가 - " + n + "일 예상저가", account_value=n_ELP23[i]), basicInfo)
        #
        # n_HLW23 = []
        # for i in self.n_list:
        #     n_HLW23.append(self.n_HLW23(k, i))
        # for i in range(len(self.n_list)):
        #     n = str(self.n_list[i])
        #     self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code=n + "HLW23", account_name="23차 예상고저가 - 고저변동폭 " + n + "일", account_value=n_HLW23[i]), basicInfo)
        #
        # n_HLMA23 = []
        # for i in self.n_list:
        #     n_HLMA23.append(self.n_HLMA23(k, i))
        # for i in range(len(self.n_list)):
        #     n = str(self.n_list[i])
        #     self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code=n + "HLMA23", account_name="23차 예상고저가 - 전5일 이평 " + n + "일", account_value=n_HLMA23[i]), basicInfo)
        #
        # n_HPEE23 = []
        # for i in self.n_list:
        #     n_HPEE23.append(self.n_HPEE23(k, i))
        # for i in range(len(self.n_list)):
        #     n = str(self.n_list[i])
        #     self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code=n + "HPEE23", account_name="23차 예측오차 - " + n + "일 고가예측오차", account_value=n_HPEE23[i]), basicInfo)
        #
        # n_LPEE23 = []
        # for i in self.n_list:
        #     n_LPEE23.append(self.n_LPEE23(k, i))
        # for i in range(len(self.n_list)):
        #     n = str(self.n_list[i])
        #     self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code=n + "LPEE23", account_name="23차 예측오차 - " + n + "일 저가예측오차", account_value=n_LPEE23[i]), basicInfo)
        #
        # _3HPEEA23 = self._3HPEEA23(k)
        # _3LPEEA23 = self._3LPEEA23(k)
        # self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code="3HPEEA23", account_name="고가예측 오차평균(3일)", account_value=_3HPEEA23), basicInfo)
        # self.insert_to_db_table_from_model(StockPricePredict(code=basicInfo, date=k, account_code="3LPEEA23", account_name="저가예측 오차평균(3일)", account_value=_3LPEEA23), basicInfo)

    def date_m(self, date, day):
        origin_date = common_util.iso_to_datetime(date)

        if self.term=="week": day = 7*day
        elif self.term=="month": day = 30*day

        result_date = origin_date - datetime.timedelta(days=day)
        return common_util.datetime_to_iso(result_date)
    def date_p(self, date, day):
        origin_date = common_util.iso_to_datetime(date)

        if self.term == "week": day = 7*day
        elif self.term == "month": day = 30*day

        result_date = origin_date + datetime.timedelta(days=day)
        return common_util.datetime_to_iso(result_date)

    def S(self, k): # 시가
        # result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code="S")
        result = pd.read_sql("select * from stock_price_predict where code_id='"+self.stock_code+"' and date='"+k+"' and account_code='S' ", self.db)
        if result.index.size != 0:
            return result.iloc[0]["account_value"]
        else:
            if self.term == "day":
                # return self.price_list.filter(date=k).get().open_price
                return self.price_list[self.price_list["date"]==k].iloc[0]["open_price"]
            elif self.term == "week":
                week_last = self.date_p(k, 1)
                # s = self.price_list.filter(date__gte=k, date__lt=week_last,).exclude(open_price=0).order_by("date")
                s = self.price_list[
                        (self.price_list["date"] >= common_util.iso_to_date(k))
                        & (self.price_list["date"] < common_util.iso_to_date(week_last))
                        & (self.price_list["open_price"] != 0)
                    ].sort_values(["date"])
                if s.index.size == 0: # 액면 분할 등으로 해당 주의 시가 데이터가 모두 0이거나 없는 경우, 전 주의 시가와 동일하다고 취급.
                    return self.S(self.date_m(k, 1))
                return s.iloc[0]["open_price"]
    def H(self, k): # 고가
        # result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code="H")
        result = pd.read_sql("select * from stock_price_predict where code_id='" + self.stock_code + "' and date='" + k + "' and account_code='H' ", self.db)
        if result.index.size != 0:
            return result.iloc[0]["account_value"]
            # return result.get().account_value
        else:
            if self.term == "day":
                # return self.price_list.filter(date=k).get().high_price
                return self.price_list[self.price_list["date"] == k].iloc[0]["high_price"]
            elif self.term == "week":
                week_last = self.date_p(k, 1)
                # week_data = self.price_list.filter(date__gte=k, date__lt=week_last).exclude(high_price=0)
                week_data = self.price_list[
                                (self.price_list["date"] >= common_util.iso_to_date(k))
                                & (self.price_list["date"] < common_util.iso_to_date(week_last))
                                & (self.price_list["high_price"] != 0)
                            ]
                if week_data.index.size == 0: # 액면 분할 등으로 해당 주의 고가 데이터가 모두 0이거나 없는 경우, 전 주의 고가와 동일하다고 취급.
                    return self.H(self.date_m(k, 1))
                # return week_data.aggregate(Max("high_price"), Min("low_price"))["high_price__max"]
                return week_data["high_price"].max()
    def L(self, k): # 저가
        result = pd.read_sql("select * from stock_price_predict where code_id='" + self.stock_code + "' and date='" + k + "' and account_code='L' ", self.db)
        if result.index.size != 0:
            return result.iloc[0]["account_value"]
        else:
            if self.term == "day":
                # return self.price_list.filter(date=k).get().low_price
                return self.price_list[self.price_list["date"] == k].iloc[0]["low_price"]
            elif self.term == "week":
                week_last = self.date_p(k, 1)
                # week_data = self.price_list.filter(date__gte=k, date__lt=week_last).exclude(low_price=0)
                week_data = self.price_list[
                                (self.price_list["date"] >= common_util.iso_to_date(k))
                                & (self.price_list["date"] < common_util.iso_to_date(week_last))
                                & (self.price_list["low_price"] != 0)
                            ]
                if week_data.index.size == 0: # 액면 분할 등으로 해당 주의 저가 데이터가 모두 0이거나 없는 경우, 전 주의 저가와 동일하다고 취급.
                    return self.L(self.date_m(k, 1))
                # return week_data.aggregate(Max("high_price"), Min("low_price"))["low_price__min"]
                return week_data["low_price"].min()
    def E(self, k): # 종가
        result = pd.read_sql("select * from stock_price_predict where code_id='" + self.stock_code + "' and date='" + k + "' and account_code='E' ", self.db)
        if result.index.size != 0:
            return result.iloc[0]["account_value"]
        else:
            if self.term == "day":
                # return self.price_list.filter(date=k).get().close_price
                return self.price_list[self.price_list["date"] == k].iloc[0]["close_price"]
            elif self.term == "week":
                week_last = self.date_p(k, 1)
                # e = self.price_list.filter(date__gte=k, date__lt=week_last).exclude(close_price=0).order_by("date")
                e = self.price_list[
                                (self.price_list["date"] >= common_util.iso_to_date(k))
                                & (self.price_list["date"] < common_util.iso_to_date(week_last))
                                & (self.price_list["close_price"] != 0)
                            ].sort_values(["date"])
                if e.index.size == 0: # 액면 분할 등으로 해당 주의 종가 데이터가 모두 0이거나 없는 경우, 전 주의 종가와 동일하다고 취급.
                    return self.E(self.date_m(k, 1))
                return e.iloc[-1]["close_price"]

    def G(self, k): # 갭등락폭
        result = pd.read_sql("select * from stock_price_predict where code_id='" + self.stock_code + "' and date='" + k + "' and account_code='G' ", self.db)
        if result.index.size != 0:
            return result.iloc[0]["account_value"]
        else:
            return self.S(k) - self.E(self.date_m(k, 1))
    def Gp(self, k): # 갭등락률
        result = pd.read_sql("select * from stock_price_predict where code_id='" + self.stock_code + "' and date='" + k + "' and account_code='Gp' ", self.db)
        if result.index.size != 0:
            return result.iloc[0]["account_value"]
        else:
            return (self.G(k)/self.E(self.date_m(k, 1))) * 100

    def HLk(self, k): # 당일고저변동폭
        # result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code="HLk")
        result = pd.read_sql("select * from stock_price_predict where code_id='" + self.stock_code + "' and date='" + k + "' and account_code='HLk' ",
                             self.db)
        if result.index.size != 0:
            return result.iloc[0]["account_value"]
        else:
            return self.H(k) - self.L(k)
    def HLk3d(self, k): #당일고저변동폭 전3일 이평
        # result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code="HLk5d")
        result = pd.read_sql("select * from stock_price_predict where code_id='" + self.stock_code + "' and date='" + k + "' and account_code='HLk5d' ",
                             self.db)
        if result.index.size != 0:
            return result.iloc[0]["account_value"]
        else:
            sum = 0
            for i in range(1, 4):
                sum = sum + self.HLk(self.date_m(k, i))
            return sum/3
    def HLk5d(self, k):
        # result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code="HLk5d")
        result = pd.read_sql("select * from stock_price_predict where code_id='" + self.stock_code + "' and date='" + k + "' and account_code='HLk5d' ",
                             self.db)
        if result.index.size != 0:
            return result.iloc[0]["account_value"]
        else:
            sum = 0
            for i in range(1, 6):
                sum = sum + self.HLk(self.date_m(k, i))
            return sum/5
    def HLk10d(self, k):
        # result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code="HLk10d")
        result = pd.read_sql("select * from stock_price_predict where code_id='" + self.stock_code + "' and date='" + k + "' and account_code='HLk10d' ",
                             self.db)
        if result.index.size != 0:
            return result.iloc[0]["account_value"]
        else:
            sum = 0
            for i in range(1, 11):
                sum = sum + self.HLk(self.date_m(k, i))
            return sum/10
    def HLk20d(self, k):
        # result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code="HLk20d")
        result = pd.read_sql("select * from stock_price_predict where code_id='" + self.stock_code + "' and date='" + k + "' and account_code='HLk20d' ",
                             self.db)
        if result.index.size != 0:
            return result.iloc[0]["account_value"]
        else:
            sum = 0
            for i in range(1, 21):
                sum = sum + self.HLk(self.date_m(k, i))
            return sum/20

    def HLpk(self, k):
        # result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code="HLpk")
        result = pd.read_sql("select * from stock_price_predict where code_id='" + self.stock_code + "' and date='" + k + "' and account_code='HLpk' ",
                             self.db)
        if result.index.size != 0:
            return result.iloc[0]["account_value"]
        else:
            return (self.HLk(k)/self.L(k))* 100
    def HLpk3d(self, k):
        # result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code="HLpk3d")
        result = pd.read_sql("select * from stock_price_predict where code_id='" + self.stock_code + "' and date='" + k + "' and account_code='HLpk3d' ",
                             self.db)
        if result.index.size != 0:
            return result.iloc[0]["account_value"]
        else:
            sum = 0
            for i in range(1, 4):
                sum = sum + self.HLpk(self.date_m(k, i))
            return sum/3
    def HLpk5d(self, k):
        # result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code="HLpk5d")
        result = pd.read_sql("select * from stock_price_predict where code_id='" + self.stock_code + "' and date='" + k + "' and account_code='HLpk5d' ",
                             self.db)
        if result.index.size != 0:
            return result.iloc[0]["account_value"]
        else:
            sum = 0
            for i in range(1, 6):
                sum = sum + self.HLpk(self.date_m(k, i))
            return sum / 5
    def HLpk10d(self, k):
        # result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code="HLpk10d")
        result = pd.read_sql("select * from stock_price_predict where code_id='" + self.stock_code + "' and date='" + k + "' and account_code='HLpk10d' ",
                             self.db)
        if result.index.size != 0:
            return result.iloc[0]["account_value"]
        else:
            sum = 0
            for i in range(1, 11):
                sum = sum + self.HLpk(self.date_m(k, i))
            return sum / 10
    def HLpk20d(self, k):
        # result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code="HLpk20d")
        result = pd.read_sql("select * from stock_price_predict where code_id='" + self.stock_code + "' and date='" + k + "' and account_code='HLpk20d' ",
                             self.db)
        if result.index.size != 0:
            return result.iloc[0]["account_value"]
        else:
            sum = 0
            for i in range(1, 21):
                sum = sum + self.HLpk(self.date_m(k, i))
            return sum / 20

    def HSk(self, k):
        # result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code="HSk")
        result = pd.read_sql("select * from stock_price_predict where code_id='" + self.stock_code + "' and date='" + k + "' and account_code='HSk' ",
                             self.db)
        if result.index.size != 0:
            return result.iloc[0]["account_value"]
        else:
            return self.H(k)-self.S(k)
    def HSk_n_d(self, k, n):
        # result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code="HSk"+str(n)+"d")
        result = pd.read_sql("select * from stock_price_predict where code_id='" + self.stock_code + "' and date='" + k + "' and account_code='" + "HSk"+str(n)+"d" + "' ",
                             self.db)
        if result.index.size != 0:
            return result.iloc[0]["account_value"]
        else:
            sum = 0
            for i in range(1, n+1):
                sum = sum + self.HSk(self.date_m(k, i))
            return sum / n

    def SLk(self, k): # 시가-저가 변동폭
        # result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code="SLk")
        result = pd.read_sql("select * from stock_price_predict where code_id='" + self.stock_code + "' and date='" + k + "' and account_code='SLk' ",
                             self.db)
        if result.index.size != 0:
            return result.iloc[0]["account_value"]
        else:
            return self.S(k)-self.L(k)
    def SLk_n_d(self, k, n):# 시가저가변동폭 전 n일 이평.
        # result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code="SLk"+str(n)+"d")
        result = pd.read_sql("select * from stock_price_predict where code_id='" + self.stock_code + "' and date='" + k + "' and account_code='" + "SLk"+str(n)+"d" + "' ",
                             self.db)
        if result.index.size != 0:
            return result.iloc[0]["account_value"]
        else:
            sum = 0
            for i in range(1, n+1):
                sum = sum + self.SLk(self.date_m(k, i))
            return sum / n

    # 1차 예상고저가
    # 1차 예상 고저가 - n일 예상 고가
    def n_EHP1(self, k, n):
        # result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code=str(n)+"EHP1")
        result = pd.read_sql("select * from stock_price_predict where code_id='" + self.stock_code + "' and date='" + k + "' and account_code='" + str(n) + "EHP1" + "' ",
                             self.db)
        if result.index.size != 0:
            return result.iloc[0]["account_value"]
        else:
            return self.S(k) + self.HSk_n_d(k, n)
    # 1차 예상 고저가 - n일 예상 저가
    def n_ELP1(self, k, n):
        # result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code=str(n)+"ELP1")
        result = pd.read_sql("select * from stock_price_predict where code_id='" + self.stock_code + "' and date='" + k + "' and account_code='" + str(n) + "ELP1" + "' ",
                             self.db)
        if result.index.size != 0:
            return result.iloc[0]["account_value"]
        else:
            return self.S(k) - self.SLk_n_d(k, n) #????? +? -?

    # 1차 예상 고저가 - 고저변동폭
    def n_HLW1(self, k, n):
        # result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code=str(n)+"HLW1")
        result = pd.read_sql("select * from stock_price_predict where code_id='" + self.stock_code + "' and date='" + k + "' and account_code='" + str(n) + "HLW1" + "' ",
                             self.db)
        if result.index.size != 0:
            return result.iloc[0]["account_value"]
        else:
            return self.n_EHP1(k, n) - self.n_ELP1(k, n)
    # 1차 예상 고저가 - 전5일 이동평균
    def n_HLMA1(self, k, n):
        # result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code=str(n)+"HLMA1")
        result = pd.read_sql("select * from stock_price_predict where code_id='" + self.stock_code + "' and date='" + k + "' and account_code='" + str(n) + "HLMA1" + "' ",
                             self.db)
        if result.index.size != 0:
            return result.iloc[0]["account_value"]
        else:
            sum =0
            for i in range(1, 6):
                sum = sum + self.n_HLW1(self.date_m(k, i), n)
            return sum / 5

    # 1차 예측오차
    # 1차 예측오차 - n일 고가예측오차
    def n_HPEE1(self, k, n):
        # result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code=str(n)+"HPEE1")
        result = pd.read_sql("select * from stock_price_predict where code_id='" + self.stock_code + "' and date='" + k + "' and account_code='" + str(n) + "HPEE1" + "' ",
                             self.db)
        if result.index.size != 0:
            return result.iloc[0]["account_value"]
        else:
            return self.n_EHP1(k, n) - self.H(k)
    # 1차 예측오차 - n일 저가예측오차
    def n_LPEE1(self, k, n):
        # result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code=str(n)+"LPEE1")
        result = pd.read_sql("select * from stock_price_predict where code_id='" + self.stock_code + "' and date='" + k + "' and account_code='"+str(n)+"LPEE1"+"' ",
                             self.db)
        if result.index.size != 0:
            return result.iloc[0]["account_value"]
        else:
            return self.n_ELP1(k, n) - self.L(k)

    # 1차 예측오차 - 고가예측오차평균(3일)
    def _3HPEEA1(self, k):
        # result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code="3HPEEA1")
        result = pd.read_sql("select * from stock_price_predict where code_id='" + self.stock_code + "' and date='" + k + "' and account_code='3HPEEA1' ",
                             self.db)
        if result.index.size != 0:
            return result.iloc[0]["account_value"]
        else:
            sum = 0
            for i in range(1, 4):
                sum = sum + self.n_HPEE1(self.date_m(k, i), 3)
            return sum/3
    # 1차 예측오차 - 저가예측오차평균(3일)
    def _3LPEEA1(self, k):
        # result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code="3LPEEA1")
        result = pd.read_sql("select * from stock_price_predict where code_id='" + self.stock_code + "' and date='" + k + "' and account_code='3LPEEA1' ",
                             self.db)
        if result.index.size != 0:
            return result.iloc[0]["account_value"]
        else:
            sum = 0
            for i in range(1, 4):
                sum = sum + self.n_LPEE1(self.date_m(k, i), 3)
            return sum/3

    # 2차 예상고저가
    # 2차 예상 고저가 - n일 예상 고가
    def n_EHP2(self, k, n):
        # result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code=str(n)+"EHP2")
        result = pd.read_sql("select * from stock_price_predict where code_id='" + self.stock_code + "' and date='" + k + "' and account_code='" + str(n) + "EHP2" + "' ",
                             self.db)
        if result.index.size != 0:
            return result.iloc[0]["account_value"]
        else:
            return self.n_EHP1(k, n) + self._3HPEEA1(k)

    # 2차 예상 고저가 - n일 예상 저가
    def n_ELP2(self, k, n):
        # result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code=str(n)+"ELP2")
        result = pd.read_sql("select * from stock_price_predict where code_id='" + self.stock_code + "' and date='" + k + "' and account_code='" + str(n) + "ELP2" + "' ",
                             self.db)
        if result.index.size != 0:
            return result.iloc[0]["account_value"]
        else:
            return self.n_ELP1(k, n) + self._3LPEEA1(k)  # ????? +? -?

    # 2차 예상 고저가 - 고저변동폭
    def n_HLW2(self, k, n):
        # result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code=str(n)+"HLW2")
        result = pd.read_sql("select * from stock_price_predict where code_id='" + self.stock_code + "' and date='" + k + "' and account_code='" + str(n) + "HLW2" + "' ",
                             self.db)
        if result.index.size != 0:
            return result.iloc[0]["account_value"]
        else:
            return self.n_EHP2(k, n) - self.n_ELP2(k, n)

    # 2차 예상 고저가 - 전5일 이동평균
    def n_HLMA2(self, k, n):
        # result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code=str(n)+"HLMA2")
        result = pd.read_sql("select * from stock_price_predict where code_id='" + self.stock_code + "' and date='" + k + "' and account_code='" + str(n) + "HLMA2" + "' ",
                             self.db)
        if result.index.size != 0:
            return result.iloc[0]["account_value"]
        else:
            sum = 0
            for i in range(1, 6):
                sum = sum + self.n_HLW2(self.date_m(k, i), n)
            return sum / 5

    # 2차 예측오차
    # 2차 예측오차 - n일 고가예측오차
    def n_HPEE2(self, k, n):
        # result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code=str(n) + "HPEE2")
        result = pd.read_sql("select * from stock_price_predict where code_id='" + self.stock_code + "' and date='" + k + "' and account_code='" + str(n) + "HPEE2" + "' ",
                             self.db)
        if result.index.size != 0:
            return result.iloc[0]["account_value"]
        else:
            return self.n_EHP2(k, n) - self.H(k)

    # 2차 예측오차 - n일 저가예측오차
    def n_LPEE2(self, k, n):
        # result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code=str(n) + "LPEE2")
        result = pd.read_sql("select * from stock_price_predict where code_id='" + self.stock_code + "' and date='" + k + "' and account_code='"+str(n) + "LPEE2"+"' ",
                             self.db)
        if result.index.size != 0:
            return result.iloc[0]["account_value"]
        else:
            return self.n_ELP2(k, n) - self.L(k)

    # 2차 예측오차 - 고가예측오차평균(3일)
    def _3HPEEA2(self, k):
        # result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code="3HPEEA2")
        result = pd.read_sql("select * from stock_price_predict where code_id='" + self.stock_code + "' and date='" + k + "' and account_code='3HPEEA2' ",
                             self.db)
        if result.index.size != 0:
            return result.iloc[0]["account_value"]
        else:
            sum = 0
            for i in range(1, 4):
                sum = sum + self.n_HPEE2(self.date_m(k, i), 3)
            return sum / 3

    # 2차 예측오차 - 저가예측오차평균(3일)
    def _3LPEEA2(self, k):
        # result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code="3LPEEA2")
        result = pd.read_sql("select * from stock_price_predict where code_id='" + self.stock_code + "' and date='" + k + "' and account_code='3LPEEA2' ",
                             self.db)
        if result.index.size != 0:
            return result.iloc[0]["account_value"]
        else:
            sum = 0
            for i in range(1, 4):
                sum = sum + self.n_LPEE2(self.date_m(k, i), 3)
            return sum / 3

    # 3차 예상고저가
    # 3차 예상 고저가 - n일 예상 고가
    def n_EHP3(self, k, n):
        # result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code=str(n) + "EHP3")
        result = pd.read_sql("select * from stock_price_predict where code_id='" + self.stock_code + "' and date='" + k + "' and account_code='" + str(n) + "EHP3" + "' ",
                             self.db)
        if result.index.size != 0:
            return result.iloc[0]["account_value"]
        else:
            return self.n_EHP2(k, n) + self._3HPEEA2(k)

    # 3차 예상 고저가 - n일 예상 저가
    def n_ELP3(self, k, n):
        # result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code=str(n) + "ELP3")
        result = pd.read_sql("select * from stock_price_predict where code_id='" + self.stock_code + "' and date='" + k + "' and account_code='" + str(n) + "ELP3" + "' ",
                             self.db)
        if result.index.size != 0:
            return result.iloc[0]["account_value"]
        else:
            return self.n_ELP2(k, n) + self._3LPEEA2(k)  # ????? +? -?

    # 3차 예상 고저가 - 고저변동폭
    def n_HLW3(self, k, n):
        # result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code=str(n) + "HLW3")
        result = pd.read_sql("select * from stock_price_predict where code_id='" + self.stock_code + "' and date='" + k + "' and account_code='" + str(n) + "HLW3" + "' ",
                             self.db)
        if result.index.size != 0:
            return result.iloc[0]["account_value"]
        else:
            return self.n_EHP3(k, n) - self.n_ELP3(k, n)

    # 3차 예상 고저가 - 전5일 이동평균
    def n_HLMA3(self, k, n):
        # result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code=str(n) + "HLMA3")
        result = pd.read_sql("select * from stock_price_predict where code_id='" + self.stock_code + "' and date='" + k + "' and account_code='" + str(n) + "HLMA3" + "' ",
                             self.db)
        if result.index.size != 0:
            return result.iloc[0]["account_value"]
        else:
            sum = 0
            for i in range(1, 6):
                sum = sum + self.n_HLW3(self.date_m(k, i), n)
            return sum / 5

    # 3차 예측오차
    # 3차 예측오차 - n일 고가예측오차
    def n_HPEE3(self, k, n):
        # result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code=str(n) + "HPEE3")
        result = pd.read_sql("select * from stock_price_predict where code_id='" + self.stock_code + "' and date='" + k + "' and account_code='" + str(n) + "HPEE3" + "' ",
                             self.db)
        if result.index.size != 0:
            return result.iloc[0]["account_value"]
        else:
            return self.n_EHP3(k, n) - self.H(k)

    # 3차 예측오차 - n일 저가예측오차
    def n_LPEE3(self, k, n):
        # result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code=str(n) + "LPEE3")
        result = pd.read_sql("select * from stock_price_predict where code_id='" + self.stock_code + "' and date='" + k + "' and account_code='" + str(n) + "LPEE3" + "' ",
                             self.db)
        if result.index.size != 0:
            return result.iloc[0]["account_value"]
        else:
            return self.n_ELP3(k, n) - self.L(k)

    # 3차 예측오차 - 고가예측오차평균(3일)
    def _3HPEEA3(self, k):
        # result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code="3HPEEA3")
        result = pd.read_sql("select * from stock_price_predict where code_id='" + self.stock_code + "' and date='" + k + "' and account_code='3HPEEA3' ",
                             self.db)
        if result.index.size != 0:
            return result.iloc[0]["account_value"]
        else:
            sum = 0
            for i in range(1, 4):
                sum = sum + self.n_HPEE3(self.date_m(k, i), 3)
            return sum / 3

    # 3차 예측오차 - 저가예측오차평균(3일)
    def _3LPEEA3(self, k):
        # result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code="3LPEEA3")
        result = pd.read_sql("select * from stock_price_predict where code_id='" + self.stock_code + "' and date='" + k + "' and account_code='3LPEEA3' ",
                             self.db)
        if result.index.size != 0:
            return result.iloc[0]["account_value"]
        else:
            sum = 0
            for i in range(1, 4):
                sum = sum + self.n_LPEE3(self.date_m(k, i), 3)
            return sum / 3

    # 123차 예상고저가
    # 123차 예상고저가 - n일 예상고가
    def n_EHP123(self, k, n):
        # result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code=str(n) + "EHP123")
        result = pd.read_sql("select * from stock_price_predict where code_id='" + self.stock_code + "' and date='" + k + "' and account_code='"+str(n) + "EHP123"+"' ", self.db)
        if result.index.size != 0:
            return result.iloc[0]["account_value"]
            # return result.get().account_value
        else:
            return (self.n_EHP1(k, n) + self.n_EHP2(k, n) + self.n_EHP3(k, n))/3
    # 123차 예상고저가 - n일 예상저가
    def n_ELP123(self, k, n):
        # result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code=str(n) + "ELP123")
        result = pd.read_sql("select * from stock_price_predict where code_id='" + self.stock_code + "' and date='" + k + "' and account_code='" + str(n) + "ELP123" + "' ",
                             self.db)
        if result.index.size != 0:
            return result.iloc[0]["account_value"]
            # return result.get().account_value
        else:
            return (self.n_ELP1(k, n) + self.n_ELP2(k, n) + self.n_ELP3(k, n)) / 3

    # # 123차 예상 고저가 - 고저변동폭
    # def n_HLW123(self, k, n):
    #     result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code=str(n) + "HLW123")
    #     if result.count() != 0:
    #         return result.get().account_value
    #     else:
    #         return self.n_EHP123(k, n) - self.n_ELP123(k, n)
    #
    # # 123차 예상 고저가 - 전5일 이동평균
    # def n_HLMA123(self, k, n):
    #     result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code=str(n) + "HLMA123")
    #     if result.count() != 0:
    #         return result.get().account_value
    #     else:
    #         sum = 0
    #         for i in range(1, 6):
    #             sum = sum + self.n_HLW123(self.date_m(k, i), n)
    #         return sum / 5
    #
    # # 123차 예측오차
    # # 123차 예측오차 - n일 고가예측오차
    # def n_HPEE123(self, k, n):
    #     result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code=str(n) + "HPEE123")
    #     if result.count() != 0:
    #         return result.get().account_value
    #     else:
    #         return self.n_EHP123(k, n) - self.H(k)
    #
    # # 123차 예측오차 - n일 저가예측오차
    # def n_LPEE123(self, k, n):
    #     result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code=str(n) + "LPEE123")
    #     if result.count() != 0:
    #         return result.get().account_value
    #     else:
    #         return self.n_ELP123(k, n) - self.L(k)
    #
    # # 123차 예측오차 - 고가예측오차평균(3일)
    # def _3HPEEA123(self, k):
    #     result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code="3HPEEA123")
    #     if result.count() != 0:
    #         return result.get().account_value
    #     else:
    #         sum = 0
    #         for i in range(1, 4):
    #             sum = sum + self.n_HPEE123(self.date_m(k, i), 3)
    #         return sum / 3
    #
    # # 123차 예측오차 - 저가예측오차평균(3일)
    # def _3LPEEA123(self, k):
    #     result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code="3LPEEA123")
    #     if result.count() != 0:
    #         return result.get().account_value
    #     else:
    #         sum = 0
    #         for i in range(1, 4):
    #             sum = sum + self.n_LPEE123(self.date_m(k, i), 3)
    #         return sum / 3
    #
    #
    # # 12차 예상고저가
    # # 12차 예상고저가 - n일 예상고가
    # def n_EHP12(self, k, n):
    #     result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code=str(n) + "EHP12")
    #     if result.count() != 0:
    #         return result.get().account_value
    #     else:
    #         return (self.n_EHP1(k, n) + self.n_EHP2(k, n)) / 2
    #
    # # 12차 예상고저가 - n일 예상저가
    # def n_ELP12(self, k, n):
    #     result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code=str(n) + "ELP12")
    #     if result.count() != 0:
    #         return result.get().account_value
    #     else:
    #         return (self.n_ELP1(k, n) + self.n_ELP2(k, n)) / 2
    #
    # # 12차 예상 고저가 - 고저변동폭
    # def n_HLW12(self, k, n):
    #     result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code=str(n) + "HLW12")
    #     if result.count() != 0:
    #         return result.get().account_value
    #     else:
    #         return self.n_EHP12(k, n) - self.n_ELP12(k, n)
    #
    # # 12차 예상 고저가 - 전5일 이동평균
    # def n_HLMA12(self, k, n):
    #     result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code=str(n) + "HLMA12")
    #     if result.count() != 0:
    #         return result.get().account_value
    #     else:
    #         sum = 0
    #         for i in range(1, 6):
    #             sum = sum + self.n_HLW12(self.date_m(k, i), n)
    #         return sum / 5
    #
    # # 12차 예측오차
    # # 12차 예측오차 - n일 고가예측오차
    # def n_HPEE12(self, k, n):
    #     result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code=str(n) + "HPEE12")
    #     if result.count() != 0:
    #         return result.get().account_value
    #     else:
    #         return self.n_EHP12(k, n) - self.H(k)
    #
    # # 12차 예측오차 - n일 저가예측오차
    # def n_LPEE12(self, k, n):
    #     result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code=str(n) + "LPEE12")
    #     if result.count() != 0:
    #         return result.get().account_value
    #     else:
    #         return self.n_ELP12(k, n) - self.L(k)
    #
    # # 12차 예측오차 - 고가예측오차평균(3일)
    # def _3HPEEA12(self, k):
    #     result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code="3HPEEA12")
    #     if result.count() != 0:
    #         return result.get().account_value
    #     else:
    #         sum = 0
    #         for i in range(1, 4):
    #             sum = sum + self.n_HPEE12(self.date_m(k, i), 3)
    #         return sum / 3
    #
    # # 12차 예측오차 - 저가예측오차평균(3일)
    # def _3LPEEA12(self, k):
    #     result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code="3LPEEA12")
    #     if result.count() != 0:
    #         return result.get().account_value
    #     else:
    #         sum = 0
    #         for i in range(1, 4):
    #             sum = sum + self.n_LPEE12(self.date_m(k, i), 3)
    #         return sum / 3
    #
    # # 23차 예상고저가
    # # 23차 예상고저가 - n일 예상고가
    # def n_EHP23(self, k, n):
    #     result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code=str(n) + "EHP23")
    #     if result.count() != 0:
    #         return result.get().account_value
    #     else:
    #         return (self.n_EHP2(k, n) + self.n_EHP3(k, n)) / 2
    #
    # # 23차 예상고저가 - n일 예상저가
    # def n_ELP23(self, k, n):
    #     result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code=str(n) + "ELP23")
    #     if result.count() != 0:
    #         return result.get().account_value
    #     else:
    #         return (self.n_ELP2(k, n) + self.n_ELP3(k, n)) / 2
    #
    # # 23차 예상 고저가 - 고저변동폭
    # def n_HLW23(self, k, n):
    #     result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code=str(n) + "HLW23")
    #     if result.count() != 0:
    #         return result.get().account_value
    #     else:
    #         return self.n_EHP23(k, n) - self.n_ELP23(k, n)
    #
    # # 23차 예상 고저가 - 전5일 이동평균
    # def n_HLMA23(self, k, n):
    #     result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code=str(n) + "HLMA23")
    #     if result.count() != 0:
    #         return result.get().account_value
    #     else:
    #         sum = 0
    #         for i in range(1, 6):
    #             sum = sum + self.n_HLW23(self.date_m(k, i), n)
    #         return sum / 5
    #
    # # 23차 예측오차
    # # 23차 예측오차 - n일 고가예측오차
    # def n_HPEE23(self, k, n):
    #     result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code=str(n) + "HPEE23")
    #     if result.count() != 0:
    #         return result.get().account_value
    #     else:
    #         return self.n_EHP23(k, n) - self.H(k)
    #
    # # 23차 예측오차 - n일 저가예측오차
    # def n_LPEE23(self, k, n):
    #     result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code=str(n) + "LPEE23")
    #     if result.count() != 0:
    #         return result.get().account_value
    #     else:
    #         return self.n_ELP23(k, n) - self.L(k)
    #
    # # 23차 예측오차 - 고가예측오차평균(3일)
    # def _3HPEEA23(self, k):
    #     result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code="3HPEEA23")
    #     if result.count() != 0:
    #         return result.get().account_value
    #     else:
    #         sum = 0
    #         for i in range(1, 4):
    #             sum = sum + self.n_HPEE23(self.date_m(k, i), 3)
    #         return sum / 3
    #
    # # 23차 예측오차 - 저가예측오차평균(3일)
    # def _3LPEEA23(self, k):
    #     result = StockPricePredict.objects.filter(code=self.stock_code, date=k, account_code="3LPEEA23")
    #     if result.count() != 0:
    #         return result.get().account_value
    #     else:
    #         sum = 0
    #         for i in range(1, 4):
    #             sum = sum + self.n_LPEE23(self.date_m(k, i), 3)
    #         return sum / 3

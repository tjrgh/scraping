import copy
import os
import random
import re
import shutil
from shutil import which
import traceback
import pandas as pd
import psycopg2
import scrapy
import datetime
import time
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, NoSuchWindowException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from . import constant_var as constant

# 테마주 종목 데이터 스크래핑.
# - 섹터보다는 짧을 것이라 생각되지만, 섹터 데이터도 최근 데이터 이외에 의미가 없다고 하므로 테마는 더더욱 없을 거라 생각됨.
#   또한 어차피 얻으려는 데이터가 분기마다 나오는 데이터임. 하여 계산할 데이터 주기는 섹터와 동일하게함.
class ThemeSpider(scrapy.Spider):
    name="theme_spider"

    def __init__(self, target_term=None, pre_target_term=None):
        super(ThemeSpider, self).__init__()
        self.target_term = target_term
        self.pre_target_term = pre_target_term

        chrome_driver = constant.chrome_driver_path
        chrome_options = Options()
        chrome_options.add_experimental_option("prefs", {
            "profile.content_settings.exceptions.automatic_downloads.*.setting": 1,
            "plugins.always_open_pdf_externally": True
        })
        self.driver = webdriver.Chrome(chrome_driver, chrome_options=chrome_options)

    def start_requests(self):
        url_list = [
            "https://finance.naver.com/"
        ]

        for url in url_list:
            yield scrapy.Request(url=url)

    def parse(self, response, **kwargs):
        self.driver.get(response.url);
        self.driver.implicitly_wait(30);

        try:
            db = psycopg2.connect(
                host="112.220.72.179", dbname="openmetric", user="openmetric", password=")!metricAdmin01", port=2345)
            cur = db.cursor()

            # 네이버 금융 페이지 이동
            temp_button = self.driver.find_element_by_xpath(
                "//div[@id='header']//div[contains(@class,'lnb_area')]//div[@id='menu']/ul/li//span[contains(text(),'국내증시')]"
            )
            self.driver.execute_script("arguments[0].click();", temp_button)
            time.sleep(random.uniform(2, 3))

            # 테마 선택.
            temp_button = self.driver.find_element_by_xpath(
                "//div[@id='newarea']//div[contains(@class,'snb')]/ul/li[1]/ul//span[contains(text(),'테마')]"
            )
            self.driver.execute_script("arguments[0].click();", temp_button)
            time.sleep(random.uniform(2, 3))
            # 테마 목록 정렬.
            temp_button = self.driver.find_element_by_xpath(
                "//div[@id='newarea']//div[@id='contentarea_left']/table[1]//tr[1]/th[1]/a"
            )
            self.driver.execute_script("arguments[0].click();", temp_button)
            time.sleep(random.uniform(2, 3))
            # 마지막 페이지 계산
            last_page = self.driver.find_element_by_xpath(
                "//div[@id='newarea']//div[@id='contentarea_left']/table[2]//td[contains(@class,'pgRR')]/preceding-sibling::td[1]"
            ).text

            # 테마 페이지에 대해 반복
            for page_num in range(1,int(last_page)+1):
                page_button = self.driver.find_element_by_xpath(
                    "//div[@id='newarea']//div[@id='contentarea_left']/table[2]//td/a[text()='"+str(page_num)+"']"
                )
                self.driver.execute_script("arguments[0].click();", page_button)
                time.sleep(random.uniform(2, 3))

                # 테마별 종목 스크래핑
                category_xpath = "//div[@id='contentarea']//div[@id='contentarea_left']/table[1]//tr/td[1]/a"
                category_count = len(self.driver.find_elements_by_xpath(category_xpath))
                # 테마 목록에 대해 반복.
                for category_count_num in range(category_count):
                    # 테마 목록 하나 클릭.
                    temp_button = self.driver.find_elements_by_xpath(category_xpath)
                    theme_name = temp_button[category_count_num].text
                    if len(theme_name) > 9:
                        continue
                    self.driver.execute_script("arguments[0].click();", temp_button[category_count_num])
                    time.sleep(random.uniform(2, 3))

                    # 수익(손), 자산총계(재), 부채총계(재), 자본총계(재), 등등...
                    # df = pd.DataFrame(columns=["code", "stock_name", "account_id", "term", "value"])  # 섹터에 대한 데이터들 저장할 df.

                    # 종목 추출
                    sector_stock_list = self.driver.find_elements_by_xpath(
                        "//div[@id='contentarea']/div[contains(@class,'box_type_l')][2]/table/tbody/tr/td[contains(@class,'name')]//a"
                    )

                    insert_sql = ""
                    # 현재 테마에 속한 종목들의 데이터 리스트
                    gross_profit_ratio_list = pd.Series()
                    operating_profit_ratio_list = pd.Series()
                    net_profit_ratio_list = pd.Series()
                    net_income_list = pd.Series()
                    ebitda_list = pd.Series()
                    roa_list = pd.Series()
                    roe_list = pd.Series()
                    eps_list = pd.Series()
                    bps_list = pd.Series()
                    gross_profit_growth_ratio_list = pd.Series()
                    operating_profit_growth_ratio_list = pd.Series()
                    net_profit_growth_ratio_list = pd.Series()
                    total_asset_growth_ratio_list = pd.Series()
                    current_asset_growth_ratio_list = pd.Series()
                    tangible_asset_growth_ratio_list = pd.Series()
                    capital_asset_growth_ratio_list = pd.Series()
                    share_price_list = pd.Series()
                    transaction_volume_list = pd.Series()
                    market_cap_list = pd.Series()
                    per_list = pd.Series()
                    pbr_list = pd.Series()


                    # stock_name_list = ["KR모터스", "경방"]
                    for stock in sector_stock_list:
                        # for i in stock_name_list:
                        #     stock_name = i
                        stock_name = stock.text

                        try:
                            cur.execute("select code from stocks_basic_info where name='" + stock_name + "' ")
                            code = cur.fetchone()[0]
                        except Exception as e:
                            continue

                        financial_data = pd.read_sql(
                            "select account_id, this_term_name, this_term_amount, code_id, subject_name from stock_financial_statement "
                            "where "
                            "   code_id='" + code + "' "
                            "   and ("
                            "       (subject_name='포괄손익계산서' "
                            "       and (this_term_name='" + self.target_term + "' or this_term_name='" + self.pre_target_term + "') "
                            "       and (account_id='1000' or account_id='3000' or account_id='5000' or account_id='8200' )) "
                            "       or "
                            "       (subject_name='재무상태표' "
                            "       and (this_term_name='" + self.target_term + "' or this_term_name='" + self.pre_target_term + "') "
                            "       and (account_id='5000' or account_id='8000' or account_id='8900' or account_id='8111' "
                            "       or account_id='2000' or account_id='3200')) "
                            "       or "
                            "       (subject_name='현금흐름표' "
                            "       and (this_term_name='" + self.target_term + "' or this_term_name='" + self.pre_target_term + "') "
                            "       and (account_id='1211' or account_id='1212')))"
                            , db)
                        financial_data = financial_data.dropna(axis=0)

                        # 시장 데이터
                        market_data = pd.read_sql(
                            "select AVG(close_price) as price_avg, AVG(transaction_volume) as transaction_volume_avg, code_id "
                            "from stocks_historic_data "
                            "where code_id='" + code + "' "
                            "   and date BETWEEN '" + self.pre_target_term + "' AND '" + self.target_term + "' "
                            "group by code_id ",
                            db
                        )

                        def set_data(subject_name, account_id, term_name):
                            data = financial_data.loc[(financial_data["subject_name"] == subject_name)  # 총 이익
                                                      & (financial_data["account_id"] == account_id)
                                                      & (financial_data["this_term_name"] == term_name)]
                            if data.empty == True:
                                data = None
                            else:
                                data = data.iloc[0]["this_term_amount"]

                            return data

                        # 데이터 계산
                        profit = set_data("포괄손익계산서", "3000", self.target_term)  # 매출 이익
                        revenue = set_data("포괄손익계산서", "1000", self.target_term)  # 매출액
                        pre_revenue = set_data("포괄손익계산서", "1000", self.pre_target_term)
                        operating_profit = set_data("포괄손익계산서", "5000", self.target_term)  # 영업이익
                        pre_operating_profit = set_data("포괄손익계산서", "5000", self.pre_target_term)
                        net_income = set_data("포괄손익계산서", "8200", self.target_term)  # 당기순이익
                        pre_net_income = set_data("포괄손익계산서", "8200", self.pre_target_term)
                        current_asset = set_data("재무상태표", "2000", self.target_term)  # 유동자산
                        pre_current_asset = set_data("재무상태표", "2000", self.pre_target_term)
                        total_asset = set_data("재무상태표", "5000", self.target_term)  # 총 자산
                        pre_total_asset = set_data("재무상태표", "5000", self.pre_target_term)
                        total_debt = set_data("재무상태표", "8000", self.target_term)  # 총부채
                        total_capital = set_data("재무상태표", "8900", self.target_term)  # 총자본
                        pre_total_capital = set_data("재무상태표", "8900", self.pre_target_term)
                        issued_share_count = set_data("재무상태표", "8111", self.target_term)  # 발행주식수
                        tangible_asset = set_data("재무상태표", "3200", self.target_term)  # 유형자산
                        pre_tangible_asset = set_data("재무상태표", "3200", self.pre_target_term)
                        a1211 = set_data("현금흐름표", "1211", self.target_term)
                        a1212 = set_data("현금흐름표", "1212", self.target_term)

                        if (profit != None) & (revenue != None):
                            gross_profit_ratio = profit / revenue * 100  # 매출 총 이익율
                            gross_profit_ratio_list = gross_profit_ratio_list.append(pd.Series(data=gross_profit_ratio))
                        if (operating_profit != None) & (revenue != None):
                            operating_profit_ratio = operating_profit / revenue * 100  # 영업이익률
                            operating_profit_ratio_list = operating_profit_ratio_list.append(
                                pd.Series(data=operating_profit_ratio))
                        if (net_income != None) & (revenue != None):
                            net_profit_ratio = net_income / revenue * 100  # 순 이익률
                            net_profit_ratio_list = net_profit_ratio_list.append(pd.Series(data=net_profit_ratio))
                        if (net_income != None):
                            net_income_list = net_income_list.append(pd.Series(data=net_income))
                        if (operating_profit != None) & (a1212 != None) & (a1211 != None):
                            ebitda = operating_profit + a1212 + a1211
                            ebitda_list = ebitda_list.append(pd.Series(data=ebitda))
                        if (net_income != None) & (total_asset != None):  # ROA
                            roa = net_income / total_asset * 100
                            roa_list = roa_list.append(pd.Series(data=roa))
                        if (net_income != None) & (total_debt != None):  # ROE
                            roe = net_income / total_debt * 100
                            roe_list = roe_list.append(pd.Series(data=roe))
                        if (net_income != None) & (issued_share_count != None):  # EPS
                            eps = net_income / issued_share_count
                            eps_list = eps_list.append(pd.Series(data=eps))
                        if (total_capital != None) & (issued_share_count != None):  # BPS
                            bps = total_capital / issued_share_count
                            bps_list = bps_list.append(pd.Series(data=bps))
                        if (revenue != None) & (pre_revenue != None):  # 매출액증가율
                            gross_profit_growth_ratio = revenue / pre_revenue
                            gross_profit_growth_ratio_list = gross_profit_growth_ratio_list.append(
                                pd.Series(data=gross_profit_growth_ratio))
                        if (operating_profit != None) & (pre_operating_profit != None):  # 영업이익증가율
                            operating_profit_growth_ratio = operating_profit / pre_operating_profit
                            operating_profit_growth_ratio_list = operating_profit_growth_ratio_list.append(
                                pd.Series(data=operating_profit_growth_ratio))
                        if (net_income != None) & (pre_net_income != None):  # 순이익증가율
                            net_profit_growth_ratio = net_income / pre_net_income
                            net_profit_growth_ratio_list = net_profit_growth_ratio_list.append(
                                pd.Series(data=net_profit_growth_ratio))
                        if (total_asset != None) & (pre_total_asset != None):  # 총자산증가율
                            total_asset_growth_ratio = total_asset / pre_total_asset
                            total_asset_growth_ratio_list = total_asset_growth_ratio_list.append(
                                pd.Series(data=total_asset_growth_ratio))
                        if (current_asset != None) & (pre_current_asset != None):  # 유동자산증가율
                            current_asset_growth_ratio = current_asset / pre_current_asset
                            current_asset_growth_ratio_list = current_asset_growth_ratio_list.append(
                                pd.Series(data=current_asset_growth_ratio))
                        if (tangible_asset != None) & (pre_tangible_asset != None):  # 유형자산증가율
                            tangible_asset_growth_ratio = tangible_asset / pre_tangible_asset
                            tangible_asset_growth_ratio_list = tangible_asset_growth_ratio_list.append(
                                pd.Series(data=tangible_asset_growth_ratio))
                        if (total_capital != None) & (pre_total_capital != None):  # 자기자본증가율
                            capital_asset_growth_ratio = total_capital / pre_total_capital
                            capital_asset_growth_ratio_list = capital_asset_growth_ratio_list.append(
                                pd.Series(data=capital_asset_growth_ratio))
                        if (market_data.empty != True) & (issued_share_count != None):  # 시가총액
                            market_cap = market_data["price_avg"][0] * issued_share_count
                            market_cap_list = market_cap_list.append(pd.Series(data=market_cap))
                        if (market_data.empty != True):  # 주가
                            share_price_list = share_price_list.append(pd.Series(data=market_data["price_avg"][0]))
                        if (market_data.empty != True):  # 거래량
                            transaction_volume_list = transaction_volume_list.append(
                                pd.Series(data=market_data["transaction_volume_avg"][0]))
                        if (market_data.empty != True) & (net_income != None) & (issued_share_count != None):  # PER
                            per = market_data["price_avg"][0] / (net_income / issued_share_count)
                            per_list = per_list.append(pd.Series(data=per))
                        if (market_data.empty != True) & (total_capital != None) & (
                                issued_share_count != None):  # PBR
                            pbr = market_data["price_avg"][0] / (total_capital / issued_share_count)
                            pbr_list = pbr_list.append(pd.Series(data=pbr))

                    temp_data = gross_profit_ratio_list.mean()
                    insert_sql = self.make_insert_query(insert_sql, theme_name, self.target_term, "gross_profit_ratio",
                                                        temp_data)

                    temp_data = operating_profit_ratio_list.mean()
                    insert_sql = self.make_insert_query(insert_sql, theme_name, self.target_term, "operating_profit_ratio",
                                                        temp_data)

                    temp_data = net_profit_ratio_list.mean()
                    insert_sql = self.make_insert_query(insert_sql, theme_name, self.target_term, "net_profit_ratio",
                                                        temp_data)

                    temp_data = net_income_list.mean()
                    insert_sql = self.make_insert_query(insert_sql, theme_name, self.target_term, "net_income", temp_data)

                    temp_data = ebitda_list.mean()
                    insert_sql = self.make_insert_query(insert_sql, theme_name, self.target_term, "EBITDA", temp_data)

                    temp_data = roa_list.mean()
                    insert_sql = self.make_insert_query(insert_sql, theme_name, self.target_term, "ROA", temp_data)

                    temp_data = roe_list.mean()
                    insert_sql = self.make_insert_query(insert_sql, theme_name, self.target_term, "ROE", temp_data)

                    # roic???

                    temp_data = eps_list.mean()
                    insert_sql = self.make_insert_query(insert_sql, theme_name, self.target_term, "EPS", temp_data)

                    temp_data = bps_list.mean()
                    insert_sql = self.make_insert_query(insert_sql, theme_name, self.target_term, "BPS", temp_data)

                    temp_data = gross_profit_growth_ratio_list.mean()
                    insert_sql = self.make_insert_query(insert_sql, theme_name, self.target_term, "gross_profit_growth_ratio",
                                                        temp_data)

                    temp_data = operating_profit_growth_ratio_list.mean()
                    insert_sql = self.make_insert_query(insert_sql, theme_name, self.target_term,
                                                        "operating_profit_growth_ratio", temp_data)

                    temp_data = net_profit_growth_ratio_list.mean()
                    insert_sql = self.make_insert_query(insert_sql, theme_name, self.target_term, "net_profit_growth_ratio",
                                                        temp_data)

                    temp_data = total_asset_growth_ratio_list.mean()
                    insert_sql = self.make_insert_query(insert_sql, theme_name, self.target_term, "total_asset_growth_ratio",
                                                        temp_data)

                    temp_data = current_asset_growth_ratio_list.mean()
                    insert_sql = self.make_insert_query(insert_sql, theme_name, self.target_term, "current_asset_growth_ratio",
                                                        temp_data)

                    temp_data = tangible_asset_growth_ratio_list.mean()
                    insert_sql = self.make_insert_query(insert_sql, theme_name, self.target_term,
                                                        "tangible_asset_growth_ratio", temp_data)

                    temp_data = capital_asset_growth_ratio_list.mean()
                    insert_sql = self.make_insert_query(insert_sql, theme_name, self.target_term, "capital_asset_growth_ratio",
                                                        temp_data)

                    # 시가총액, per, pbr, ev, 거래량 데이터 계산.
                    temp_data = market_cap_list.mean()
                    insert_sql = self.make_insert_query(insert_sql, theme_name, self.target_term, "market_cap",
                                                        temp_data)  # 시총

                    temp_data = per_list.mean()
                    insert_sql = self.make_insert_query(insert_sql, theme_name, self.target_term, "PER", temp_data)

                    temp_data = pbr_list.mean()
                    insert_sql = self.make_insert_query(insert_sql, theme_name, self.target_term, "PBR", temp_data)

                    # EV ???

                    temp_data = transaction_volume_list.mean()
                    insert_sql = self.make_insert_query(insert_sql, theme_name, self.target_term, "transaction_volume_avg",
                                                        temp_data)  # 거래량

                    cur.execute(
                        "insert into stock_sector_statement("
                        "   created_at, updated_at, sector_name, date, account_name, amount"
                        ") "
                        "values " + insert_sql[1:]
                    )
                    db.commit()

                    # 업종 목록으로 뒤로가기.
                    self.driver.back()
                    time.sleep(random.uniform(2, 3))
        except Exception as e:
            date_time = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time()))
            with open(constant.error_file_path + "/theme_error_list_" + time.strftime("%Y-%m-%d", time.localtime(
                    time.time())) + ".txt", "a", encoding="UTF-8") as f:
                f.write(date_time + "\n")
                f.write(traceback.format_exc())

    def make_insert_query(self, insert_sql, theme_name, date, account_name, amount):
        insert_sql = (insert_sql + ", ('" + str(datetime.datetime.now(datetime.timezone.utc)) + "', '" +
                      str(datetime.datetime.now(datetime.timezone.utc)) + "', '" + theme_name + "', " +
                      "'" + date + "', '" + account_name + "', '" + str(amount) + "')"
                      )
        return insert_sql

    def report_error(self, company):
        date_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
        with open(
                constant.error_file_path + "/document_error_list_" + time.strftime("%Y-%m-%d", time.localtime(
                    time.time())) + ".txt",
                "a", encoding="UTF-8") as f:
            f.write("\n")
            f.write(date_time + "_" + company["단축코드"] + "_" + company["한글 종목약명"] + "\n")
            f.write(traceback.format_exc())
            f.write("\n")

    def report_fail_list(self, company):
        self.document_fail_list = self.document_fail_list.append(
            {"단축코드": company["단축코드"], "한글 종목약명": company["한글 종목약명"]},
            ignore_index=True)
        self.document_fail_list.to_excel(constant.document_folder_path+"/document_fail_list.xlsx",
                                         index=False)

    def restart_chrome_driver(self):
        self.driver.quit()
        chrome_driver = constant.chrome_driver_path
        chrome_options = Options()
        chrome_options.add_experimental_option("prefs", {
            "profile.content_settings.exceptions.automatic_downloads.*.setting": 1
        })
        self.driver = webdriver.Chrome(chrome_driver, chrome_options=chrome_options)
        self.driver.get('https://www.deepsearch.com/?auth=login')
        self.driver.implicitly_wait(30)
        # 로그인
        for i in range(3):
            try:
                self.driver.find_element_by_xpath(
                    "//div[contains(@class,'login-page')]//div[@class='login-container']//input[@placeholder='계정']").send_keys(
                    "sooryong@gmail.com")
                time.sleep(random.uniform(2, 3))
                self.driver.find_element_by_xpath(
                    "//div[contains(@class,'login-page')]//div[@class='login-container']//input[@placeholder='비밀번호']").send_keys(
                    ")!kaimobile01")
                time.sleep(random.uniform(2, 3))
                self.driver.find_element_by_xpath(
                    "//div[contains(@class,'login-page')]//div[@class='login-container']//input[@class='button login']").click()
                time.sleep(random.uniform(3, 4))
                self.driver.refresh()
            except Exception as e2:
                print(e2)
                continue
            else:
                break
        # 메뉴바 클릭.
        menu_bar_button = self.driver.find_element_by_xpath(
            "//div[@class='deepsearch-appbar']//div[contains(@class,'app-bar-drawer')]")
        self.driver.execute_script("arguments[0].click();", menu_bar_button)
        time.sleep(random.uniform(2, 3))
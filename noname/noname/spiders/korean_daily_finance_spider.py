import copy
import os
import random
import re
import shutil
import traceback
from shutil import which

import numpy as np
import pandas as pd
import psycopg2
import scrapy
import datetime
import time
# from scrapy_selenium import SeleniumRequest
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, NoSuchWindowException, ElementNotInteractableException, TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import pymongo
from . import constant_var as constant
from . import common_util as cm

SELENIUM_DRIVER_NAME = 'chrome'
SELENIUM_DRIVER_EXECUTABLE_PATH = which('geckodriver')
SELENIUM_DRIVER_ARGUMENTS=['--headless']  # '--headless' if using chrome instead of firefox

# 분기 종료일?을 입력받아 해당 분기의 데이터를 스크래핑하는 스파이더.
class KoreanDailyFinanceSpider(scrapy.Spider):
    name = "korean_daily_finance_spider";

    def __init__(self, quarter=None):
        super(KoreanDailyFinanceSpider, self).__init__()
        chrome_driver = constant.chrome_driver_path
        chrome_options = Options()
        # # chrome_options.add_argument("start-maximized")
        chrome_options.add_experimental_option("prefs", {
            "download.default_directory": constant.download_path.replace("/", "\\") + "\\deepSearch",
            "profile.content_settings.exceptions.automatic_downloads.*.setting": 1
        })
        self.driver = webdriver.Chrome(chrome_driver, chrome_options=chrome_options)
        self.quarter = quarter
        self.scraping_count_goal = 300
        self.search_count = 0

        self.db = psycopg2.connect(host="112.220.72.179", dbname="openmetric", user="openmetric",
                              password=")!metricAdmin01", port=2345)
        self.cur = self.db.cursor()

        # self.cur.execute("select * from stocks_basic_info where corp_code != ' '")
        # self.kospi_list = self.cur.fetchall()
        self.kospi_list = pd.read_sql("select * from stocks_basic_info where corp_code!=' '", self.db).sort_values(by="code",ascending=False)

        # 시작시간, 중간 쉬는 시간, 종료시간 설정.
        self.start_time = datetime.time(int(random.triangular(9, 10, 9)),
                                        int(random.randrange(0, 59, 1)))
        self.break_time = datetime.time(int(random.triangular(12, 13, 13)),
                                        int(random.randrange(0, 59, 1)))
        self.end_time = datetime.time(int(random.triangular(22, 23, 23)),
                                      int(random.randrange(0, 59, 1)))

    def start_requests(self):
        url_list = [
            "https://www.deepsearch.com/?auth=login"
        ]

        for url in url_list:
            yield scrapy.Request(url=url)

    def parse(self, response):
        cm.wait(1)
        self.driver.get(response.url);
        self.driver.implicitly_wait(30);

        self.initial_setting()

        self.quarterly_finance_scraping()
        self.driver.quit()

    # 분기별 재무정보 스크래핑 후 업데이트
    def quarterly_finance_scraping(self):
        # 디버깅용
        self.kospi_list = self.kospi_list[200:]
        # self.kospi_list = self.kospi_list[self.kospi_list["code"]=="A293780"]

        date = self.quarter

        # 분기 데이터 받아야 하는 리스트 대상으로 한번 반복.
        # 임시 항목 리스트에 대해 분기 데이터 추출.
        for index, company in self.kospi_list.iterrows():
            search_result = True
            # 실패시 해당 종목을 3번까지 반복.
            for try_count in range(3):
                if search_result == False:
                    break;

                # db에 이미 해당 분기에 해당하는 데이터 row가 있는지 확인. 기간과 단축코드로 검색하여 row가 하나라도 있다면 이미 업데이트한 종목이라 판단.
                self.cur.execute("select * from stock_financial_statement "
                                 "where code_id='" + str(company["code"]) + "' and this_term_name='" + date + "' "
                                                                                                              "and subject_name='포괄손익계산서'")
                pre_pl = self.cur.fetchone()
                self.cur.execute("select * from stock_financial_statement "
                                 "where code_id='" + str(company["code"]) + "' and this_term_name='" + date + "' "
                                                                                                              "and subject_name='재무상태표'")
                pre_bs = self.cur.fetchone()
                self.cur.execute("select * from stock_financial_statement "
                                 "where code_id='" + str(company["code"]) + "' and this_term_name='" + date + "' "
                                                                                                              "and subject_name='현금흐름표'")
                pre_cf = self.cur.fetchone()

                if (None != pre_pl) and (None != pre_bs) and (None != pre_cf):  # 결과값이 있다면,
                    # date_time = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time()))
                    # with open("./quarterly_error_list_" + time.strftime("%Y-%m-%d",
                    #           time.localtime(time.time())) + ".txt", "a", encoding="UTF-8") as f:
                    #     f.write(date_time + "_" + company["단축코드"] + "_" + company["한글 종목약명"] + "\n")
                    #     f.write("이미 해당 데이터가 존재합니다. \n")
                    # 목록에서 제외
                    # index = self.kospi_list.loc[(self.kospi_list["단축코드"] == company["단축코드"])].index
                    # self.kospi_list = self.kospi_list.drop(index)
                    # self.kospi_list.to_excel("C:/Users/kai/Desktop/quarterly_data_list_" + self.quarter + ".xlsx",
                    #                          index=False)
                    break

                try:
                    # '기업검색'항목 이동
                    menu1_button = self.driver.find_element_by_xpath(
                        "//div[@class='deepsearch-app']/div[contains(@class,'drawer-container-layout')]"
                        "/div[contains(@class,'drawer-container')]/div[contains(@class,'drawer-container-inner')]"
                        "/div[contains(@class,'simplebar-wrapper')]/div[contains(@class,'simplebar-mask')]"
                        "//div[contains(@class,'menu-item-group')][2]/div[contains(@class,'menu-item')][3]")
                    self.driver.execute_script("arguments[0].click();", menu1_button)
                    cm.wait(2)

                    # 통합검색창 기업 단축코드 검색.
                    search_bar = self.driver.find_element_by_xpath(
                        "//input[@id='search-input']"
                    )
                    search_bar.send_keys("")
                    cm.wait(2)
                    search_bar.send_keys(company["code"][1:])
                    cm.wait(2)
                    search_bar.send_keys(Keys.RETURN)

                    # 기업 검색 결과 확인..
                    if self.check_element(
                        "//div[@id='info-list']/div[contains(@class,'search-company-info-view')]", wait_time=30
                    )==False:
                        search_result = False
                        break

                    self.search_count = self.search_count + 1

                    button = self.driver.find_element_by_xpath(
                        "//div[@id='drawer-content-layout']//div[contains(@class,'deepsearch-content')]"
                        "//div[@id='info-list']//div[contains(@class,'search-company-info-view')]"
                        "/div[contains(@class,'company-info-header')]/a"
                    )
                    self.driver.execute_script("arguments[0].click();", button)
                    cm.wait(7)

                    # 재무정보
                    button = self.driver.find_element_by_xpath(
                        "//div[@id='drawer-content-layout']//div[contains(@class,'deepsearch-content')]" \
                        "//div[contains(@class,'company-header')]/div[contains(@class,'tabs')]" \
                        "/a[contains(text(),'재무')]"
                    )
                    self.driver.execute_script("arguments[0].click();", button)
                    cm.wait(7)

                    quarterly_data_exist = False

                    # 포괄손익계산서, 재무상태표, 현금흐름표
                    for i1 in range(3, 4):
                        # 연간,분기 선택
                        finance_xpath = "//div[@id='drawer-content-layout']//div[contains(@class,'deepsearch-content')]"\
                            "//div[contains(@class,'card-layout')][2]/div[contains(@class,'company-statements-details')]"
                        option_xpath = "//div[@id='drawer-content-layout']/div[contains(@class,'deepsearch-content')]"\
                            "//div[contains(@class,'card-layout')][2]/div[contains(@class,'company-statements-details')]"\
                            "/div[contains(@class,'header')]/div[contains(@class,'date-option')]"

                        # 데이터 단위 선택.
                        button = self.driver.find_element_by_xpath("//div[@id='export-financial-container-choose']"
                                       "/div[contains(@class,'dropdown')][2]/div")
                        self.driver.execute_script("arguments[0].click();", button)
                        cm.wait(2)
                        # 분기(3개월) 선택.
                        button = self.driver.find_element_by_xpath(
                            "//div[@id='root']/div[contains(@class,'deepsesarch-dropdown-container')]"
                            "/div[contains(@class,'deepsesarch-dropdown-items')]/div[contains(@class,'items')]"
                            "/div[" + str(i1) + "]"
                        )
                        data_term = button.find_element_by_xpath("./div").text
                        self.driver.execute_script("arguments[0].click();", button)
                        # time.sleep(random.uniform(self.motion_term + 17, self.motion_term + 18))

                        for i2 in range(2, 3):
                            # 연결,개별 선택
                            data_type = self.driver.find_element_by_xpath( # 연결, 개별 선택하지 않음. 처음 설정되어있는 값이 해당 기업의 재무데이터를 보여주는 적절한 기준이라 판단.
                                "//div[@id='export-financial-container-choose']/div[contains(@class,'dropdown')][1]/div/div[contains(@class,'text')]"
                            ).text

                            # # 연결 선택.
                            # button = self.driver.find_element_by_xpath(
                            #     "//div[@id='export-financial-container-choose']/div[contains(@class, 'dropdown')][1]/div"
                            # )
                            # self.driver.execute_script("arguments[0].click();", button)
                            # cm.wait(2)
                            #
                            # button = self.driver.find_element_by_xpath(
                            #     "//div[@id='root']/div[contains(@class,'deepsesarch-dropdown-container')]"
                            #     "/div[contains(@class,'deepsesarch-dropdown-items')]/div[contains(@class,'items')]"
                            #     "/div[" + str(i2) + "]"
                            # )
                            # data_type = button.find_element_by_xpath(
                            #     "//div[@id='root']/div[contains(@class,'deepsesarch-dropdown-container')]"
                            #     "/div[contains(@class,'deepsesarch-dropdown-items')]/div[contains(@class,'items')]"
                            #     "/div[" + str(i2) + "]"
                            # ).text
                            # self.driver.execute_script("arguments[0].click();", button)

                            # 데이터 로딩 완료 확인.. 1분동안 로딩 안되면 다시 검색?
                            if self.check_element(
                                finance_xpath+"/div[contains(@class,'fundamental-table-content')]"
                                "/div[contains(@class,'react-table-layout')]"
                                "/div[contains(@class,'financial-status-table')]"
                                "/div[contains(@class,'loading')]",
                                condition=EC.invisibility_of_element_located,
                                wait_time=60
                            )==False:
                                # search_result = False
                                break
                            # 데이터 없는 경우 확인. 없으면 '개별' 재무제표 선택.
                            if self.check_element(
                                finance_xpath+"/div[contains(@class,'fundamental-table-content')]"
                                "/div[contains(@class,'react-table-layout')]"
                                "/div[contains(@class,'financial-status-table')]"
                                "/div[contains(@class,'rt-noData')]",
                                condition=EC.invisibility_of_element_located,
                                # wait_time=5
                            )==False:
                                search_result = False
                                continue

                            # 분기데이터 존재하는지 확인.
                            last_quarter_column = self.driver.find_element_by_xpath(
                                "//div[@id='financial-header']/div[contains(@class,'rt-tr')]" \
                                "/div[contains(@class,'rt-resizable-header')][last()]"
                                "/div[contains(@class,'rt-resizable-header-content')]"
                            )
                            if date[:7].replace("-", "/") in last_quarter_column.text:
                                quarterly_data_exist = True
                            else:
                                search_result = False
                                break

                            # 딥서치 사이트 기업명 확인
                            stock_name = self.driver.find_element_by_xpath(
                                "//div[@id='drawer-content-layout']//div[contains(@class,'deepsearch-content')]"
                                "//div[contains(@class,'company-header')]/div[contains(@class,'company-info')]"
                                "/div[contains(@class,'name')]"
                            ).text

                            #   포괄손익계산서 다운
                            button = self.driver.find_element_by_xpath(
                                "//div[@id='export-financial-container-choose']/div[4]//span[contains(@class,'table-export-button')]"
                            )
                            self.driver.execute_script("arguments[0].click();", button)
                            cm.wait(5)
                            if os.path.isfile(constant.download_path+"/deepSearch/" + str(company["name"]) + "-포괄손익계산서-" +
                                              data_term + "_" + data_type + ".xlsx"):
                                os.remove(constant.download_path+"/deepSearch/" + str(company["name"]) + "-포괄손익계산서-" +
                                          data_term + "_" + data_type + ".xlsx")
                            os.rename(
                                constant.download_path+"/deepSearch/DeepSearch-포괄손익계산서Table-Export.xlsx",
                                constant.download_path+"/deepSearch/" + str(
                                    company["name"]) + "-포괄손익계산서-" + data_term + "_" + data_type + ".xlsx")

                            #   재무상태표 다운
                            if i1 != 2:
                                button = self.driver.find_element_by_xpath(
                                    finance_xpath + "/div[contains(@class,'type-options')]/div[2]"
                                )
                                self.driver.execute_script("arguments[0].click();", button)
                                cm.wait(5)
                                button = self.driver.find_element_by_xpath(
                                   "//div[@id='export-financial-container-choose']/div[4]//span[contains(@class,'table-export-button')]"
                               )
                                self.driver.execute_script("arguments[0].click();", button)
                                cm.wait(5)

                                if os.path.isfile(constant.download_path+"/deepSearch/" + str(
                                        company["name"]) + "-재무상태표-" + data_term + "_" + data_type + ".xlsx"):
                                    os.remove(constant.download_path+"/deepSearch/" + str(
                                        company["name"]) + "-재무상태표-" + data_term + "_" + data_type + ".xlsx")
                                os.rename(constant.download_path+"/deepSearch/DeepSearch-재무상태표Table-Export.xlsx",
                                          constant.download_path+"/deepSearch/" + str(
                                              company["name"]) + "-재무상태표-" + data_term + "_" + data_type + ".xlsx")
                            #   현금흐릅표 다운
                            button = self.driver.find_element_by_xpath(
                                finance_xpath + "/div[contains(@class,'type-options')]/div[3]"
                            )
                            self.driver.execute_script("arguments[0].click();", button)
                            cm.wait(5)
                            button = self.driver.find_element_by_xpath(
                               "//div[@id='export-financial-container-choose']/div[4]//span[contains(@class,'table-export-button')]"
                            )
                            self.driver.execute_script("arguments[0].click();", button)
                            cm.wait(5)
                            if os.path.isfile(constant.download_path+"/deepSearch/" + str(
                                    company["name"]) + "-현금흐름표-" + data_term + "_" + data_type + ".xlsx"):
                                os.remove(constant.download_path+"/deepSearch/" + str(
                                    company["name"]) + "-현금흐름표-" + data_term + "_" + data_type + ".xlsx")
                            os.rename(
                                constant.download_path+"/deepSearch/DeepSearch-현금흐름표Table-Export.xlsx",
                                constant.download_path+"/deepSearch/" + str(
                                    company["name"]) + "-현금흐름표-" + data_term + "_" + data_type + ".xlsx")

                    if quarterly_data_exist == True:
                        # 분기 데이터 엑셀에서 추출하여 저장
                        # 포괄손익계산서
                        pl = pd.read_excel(constant.download_path+"/deepSearch/" + company["name"] + "-포괄손익계산서-분기(3개월)_"+data_type+".xlsx")
                        # 재무상태표
                        bs = pd.read_excel(constant.download_path+"/deepSearch/" + company["name"] + "-재무상태표-분기(3개월)_"+data_type+".xlsx")
                        # 현금흐름표
                        cf = pd.read_excel(constant.download_path+"/deepSearch/" + company["name"] + "-현금흐름표-분기(3개월)_"+data_type+".xlsx")

                        # db에 저장.
                        self.cur.execute("select corp_code from stocks_basic_info where code='" + company["code"] + "' ")
                        corp_code = self.cur.fetchone()[0]

                        insert_sql = ""
                        insert_sql = self.store_quarterly_data(company, pl, "포괄손익계산서", date, corp_code)
                        insert_sql = insert_sql + ", "+self.store_quarterly_data(company, bs, "재무상태표", date, corp_code)
                        insert_sql = insert_sql + ", "+self.store_quarterly_data(company, cf, "현금흐름표", date, corp_code)

                        self.cur.execute("INSERT INTO stock_financial_statement ("
                                         "created_at, updated_at, corp_code, business_year, business_month, this_term_name, "
                                         "subject_name, account_id, account_name, "
                                         "account_level, this_term_amount, ordering, code_id, parent_account_id) "
                                         "VALUES " + insert_sql)
                        self.db.commit()

                except NoSuchWindowException as e:
                    self.restart_chrome_driver()

                    # 에러 정보 저장.
                    date_time = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time()))
                    with open(constant.error_file_path+"/quarterly_error_list_"+time.strftime("%Y-%m-%d", time.localtime(time.time()))+".txt", "a", encoding="UTF-8") as f:
                        f.write(date_time + "_" + company["code"][1:] + "_" + company["name"] + "\n")
                        f.write(traceback.format_exc())
                    continue
                except Exception as e:
                    date_time = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time()))
                    with open(
                            constant.error_file_path+"/quarterly_error_list_" + time.strftime("%Y-%m-%d", time.localtime(time.time())) + ".txt",
                            "a", encoding="UTF-8") as f:
                        f.write(date_time + "_" + company["code"][1:] + "_" + company["name"] + "\n")
                        f.write(traceback.format_exc())
                    continue
                # 성공시 다음 종목 스크래핑 수행.
                # else:
                    # if quarterly_data_exist == True:
                    #     # 분기 데이터 추출 성공항 종목은 csv파일에서 제외.
                    #     index = self.kospi_list.loc[(self.kospi_list["단축코드"] == company["단축코드"])].index
                    #     self.kospi_list = self.kospi_list.drop(index)
                    #     self.kospi_list.to_excel("C:/Users/kai/Desktop/quarterly_data_list_"+self.quarter+".xlsx",index=False)
                    #     # 성공 목록에 추가.
                    #     # quarterly_complete_list = quarterly_complete_list.append({"단축코드":company["단축코드"], "한글 종목약명":company["한글 종목약명"]}, ignore_index=True)
                    #     # quarterly_complete_list.to_excel("./quarterly_complete_list_"+self.quarter+".xlsx", index=False)
                    #     break;
                finally:
                    if self.search_count % 30 == 0:
                        self.restart_chrome_driver()
                    self.search_count = cm.wait(30, search_count=self.search_count, search_count_max=self.scraping_count_goal)["search_count"]


    def store_quarterly_data(self, company, df, subject_name, date, corp_code):

        # dataframe의 컬럼명 변경.
        df.columns = df.loc[0]
        df = df.drop([0])

        # 해당 파일에 대한 데이터 존재하는지 확인.
        self.cur.execute("select * from stock_financial_statement "
                         "where code_id='" + str(company["code"]) + "' and this_term_name='" + date + "' "
                              "and subject_name='" + subject_name + "'")
        if (None != self.cur.fetchone()):  # 이미 해당 데이터가ㅓ 존재한다면,
            date_time = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time()))
            with open(constant.error_file_path + "/quarterly_error_list_" + time.strftime("%Y-%m-%d",
                          time.localtime(time.time())) + ".txt", "a", encoding="UTF-8") as f:
                f.write(date_time + "_" + company["code"][1:] + "_" + company["name"] + "\n")
                f.write(subject_name + "가 이미 존재합니다. \n")
            return ""
        elif (date not in df.columns):
            date_time = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time()))
            with open(constant.error_file_path + "/quarterly_error_list_" + time.strftime("%Y-%m-%d",
                          time.localtime( time.time())) + ".txt", "a", encoding="UTF-8") as f:
                f.write(date_time + "_" + company["code"][1:] + "_" + company["name"] + "\n")
                f.write(subject_name + "에 "+date+"분기 데이터가 존재하지 않습니다.  \n")
            return ""

        # 재무 엑셀 파일에서 누락된 필수 항목들 체크 후, 계산하여 삽입.
        if subject_name == "포괄손익계산서":
            # 주당순이익이 없는 경우.
            if (df["account_id"] == "8200").any() == False:
                account_id = ""
                if (df["account_id"] == "8110").any() == True:
                    account_id = "8110"
                elif (df["account_id"] == "8160").any() == True:
                    account_id = "8160"
                elif (df["account_id"] == "8290").any() == True:
                    account_id = "8290"
                else:
                    raise Exception("총당기순이익 데이터 및 대체 데이터가 존재하지 않습니다.")

                temp_row = df[df["account_id"] == account_id]
                temp_row.iloc[0]["account_id"] = "8200"
                temp_row.iloc[0]["계정명"] = "총당기순이익"
                df.append(temp_row)

        elif subject_name == "재무상태표":
            pass;
        elif subject_name == "현금흐름표":
            pass;

        insert_sql = ""
        for row in df.index:  # 엑셀 파일의 row들에 대해 반복.
            # this_term_amount
            amount = df.loc[row][date]
            if np.isnan(df.loc[row][date]):
                amount = 'null'
            # account_name
            account_name = df.loc[row]["계정명"].replace(" ", "")
            parent_account_id = ""
            lv = int(df.loc[row]["LV"])
            row_sub_count = 1
            while lv != 0:
                if lv - 1 != int(df.loc[row - row_sub_count]["LV"]):
                    row_sub_count = row_sub_count + 1
                    continue
                if lv == int(df.loc[row]["LV"]):
                    parent_account_id = df.loc[row - row_sub_count]["account_id"]
                lv = int(df.loc[row - row_sub_count]["LV"])
                account_name = df.loc[row - row_sub_count]["계정명"].replace(" ", "") + "_" + account_name
                row_sub_count = row_sub_count + 1

            sql_value = ("(" +
                 "'" + str(datetime.datetime.now(datetime.timezone.utc)) + "', '" + str(datetime.datetime.now(datetime.timezone.utc)) + "', " +
                 "'" + corp_code + "', '" + date.split("-")[0] + "', '" + date.split("-")[1] + "', '" + date + "', '" + subject_name + "', " +
                 "'" + str(df.loc[row]["account_id"]) + "', '" + account_name + "', '" + str(
                df.loc[row]["LV"]) + "', " +
                 "" + str(amount) + ", '" + str(df.loc[row]["LV"]) + "', '" + str(company["code"]) + "', '"+parent_account_id+"')")
            insert_sql = insert_sql + ", " + sql_value

        insert_sql = insert_sql[1:]
        return insert_sql

    def report_error(self, e=None, code="", stock_name="", msg=""):
        date_time = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time()))
        with open(constant.error_file_path + "/quarterly_error_list_" +
                  time.strftime("%Y-%m-%d", time.localtime(time.time())) + ".txt", "a", encoding="UTF-8") as f:
            f.write(date_time + "_" + code + "_" + stock_name +"_" + msg + "\n")
            f.write(traceback.format_exc())

    def check_element(self, xpath, condition=EC.visibility_of_element_located, wait_time=5):
        try:
            WebDriverWait(self.driver, wait_time).until(condition((By.XPATH, xpath)))
            cm.wait(3)
            return True
        except TimeoutException as e:
            return False

    def click_element(self, xpath, wait_time, term=None):
        button = self.driver.find_element_by_xpath(xpath)
        try:
            button.click()
        except ElementNotInteractableException as e:
            self.driver.execute_script("arguments[0].click();", button)
        # except Exception as e:
        #     self.driver.execute_script("arguments[0].click();", button)
        if term == None:
            cm.wait(wait_time)
        else:
            cm.wait(wait_time, term)

    def wait(self, wait_time, term=5, search_count=None):
        now = datetime.datetime.now().time()
        # 검색 쿼리 횟수 제한
        if (search_count != None):
            if (search_count >= self.scraping_count_goal):
                print("search count limit break term start.")
                self.report_error(msg="search count limit break term start.")
                while (datetime.datetime.now().time() < datetime.time(6, 0)) | \
                    (datetime.datetime.now().time() > datetime.time(7, 0)):
                    time.sleep(10)
                else:
                    print("search count limit break term end.")
                    self.report_error(msg="search count limit break term end.")
                    self.start_time = datetime.time(int(random.triangular(9, 10, 9)),
                                                        int(random.randrange(0, 59, 1)))
                    self.break_time = datetime.time(int(random.triangular(12, 13, 13)),
                                                        int(random.randrange(0, 59, 1)))
                    self.end_time = datetime.time(int(random.triangular(17, 19, 18)),
                                                        int(random.randrange(0, 59, 1)))
                    self.search_count = 0

        # 시작시간, 중간 쉬는 시간, 끝시간에 따른 대기.
        if (self.start_time > now) | (self.end_time < now):
            print("start break term start.")
            self.report_error(msg="start break term start.")
            while (self.start_time > datetime.datetime.now().time()) | \
                    (self.end_time < datetime.datetime.now().time()):
                time.sleep(10)
            else:
                print("start break term end.")
                self.report_error(msg="start break term end.")
                self.start_time = datetime.time(int(random.triangular(9, 10, 9)),
                                                int(random.randrange(0, 59, 1)))
                self.search_count = 0
                self.end_time = datetime.time(int(random.triangular(17, 19, 19)),
                                              int(random.randrange(0, 59, 1)))
                self.report_error(msg=("start_time : " + str(self.start_time)))

        elif (self.break_time < now) & \
                ((datetime.datetime.combine(datetime.date.today(),self.break_time)
                 + datetime.timedelta(minutes=30)).time() > now):
            print("middle break term start.")
            self.report_error(msg="middle break term start.")
            time.sleep(random.normalvariate(3000, 300))
            print("middle break term end.")
            self.report_error(msg="middle break term end.")
            self.break_time = datetime.time(int(random.triangular(12, 13, 13)),
                                            int(random.randrange(0, 59, 1)))
            self.report_error(msg=("break_time : " + str(self.break_time)))

        # 랜덤 몇 초 더 대기.
        random_value = random.randrange(1, 100, 1)
        if random_value % 20 == 0:
            print("more sleep...")
            time.sleep(random.triangular(wait_time, wait_time + term + 10, wait_time + term + 5))
        time.sleep(random.triangular(wait_time, wait_time + term, wait_time))
        # 랜덤 3~5분 대기.
        random_value3 = random.randrange(1, 100, 1)
        if random_value3 % 100 == 0:
            print("3~5minute sleep")
            self.report_error(msg="3~5minute sleep")
            time.sleep(random.uniform(180, 300))
        # 랜덤 10~20분 대기.
        random_value2 = random.randrange(1, 1000, 1)
        if random_value2 % 500 == 0:
            print("10~20minute sleep")
            self.report_error(msg="10~20minute sleep")
            time.sleep(random.uniform(600, 1200))

    def initial_setting(self):
        for i in range(10):
            try:
                cm.wait(1)

                # 로그인
                self.driver.find_element_by_xpath(
                    "//div[contains(@class,'login-page')]//div[@class='login-container']//input[@placeholder='계정']").send_keys(
                    "sooryong@gmail.com")
                cm.wait(2+i)
                self.driver.find_element_by_xpath(
                    "//div[contains(@class,'login-page')]//div[@class='login-container']//input[@placeholder='비밀번호']").send_keys(
                    ")!kaimobile01")
                cm.wait(2+i)
                self.driver.find_element_by_xpath(
                    "//div[contains(@class,'login-page')]//div[@class='login-container']//input[@class='button login']").click()
                cm.wait(3+i)
                self.driver.refresh()
                cm.wait(2+i)

                # 메뉴바 클릭.
                menu_bar_button = self.driver.find_element_by_xpath(
                    "//div[@class='deepsearch-appbar']//div[contains(@class,'app-bar-drawer')]")
                self.driver.execute_script("arguments[0].click();", menu_bar_button)
                cm.wait(2+i)

                # '기업검색'항목 이동
                menu1_button = self.driver.find_element_by_xpath(
                    "//div[@class='deepsearch-app']/div[contains(@class,'drawer-container-layout')]"
                    "/div[contains(@class,'drawer-container')]/div[contains(@class,'drawer-container-inner')]"
                    "/div[contains(@class,'simplebar-wrapper')]/div[contains(@class,'simplebar-mask')]"
                    "//div[contains(@class,'menu-item-group')][2]/div[contains(@class,'menu-item')][3]")
                self.driver.execute_script("arguments[0].click();", menu1_button)
                cm.wait(2+i)

            except Exception as e:
                date_time = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time()))
                with open(
                        constant.error_file_path+"/quarterly_error_list_" + time.strftime("%Y-%m-%d", time.localtime(time.time())) + ".txt",
                        "a", encoding="UTF-8") as f:
                    f.write(date_time + "_초기 세팅 실패.\n")
                    f.write(traceback.format_exc())
                self.driver.refresh()
                cm.wait(2+i)
                continue
            else:
                break

    def restart_chrome_driver(self):
        self.driver.quit()
        chrome_driver = constant.chrome_driver_path
        chrome_options = Options()
        chrome_options.add_experimental_option("prefs", {
            "download.default_directory": constant.download_path.replace("/", "\\") + "\\deepSearch",
            "profile.content_settings.exceptions.automatic_downloads.*.setting": 1
        })
        self.driver = webdriver.Chrome(chrome_driver, chrome_options=chrome_options)
        self.driver.get('https://www.deepsearch.com/?auth=login')
        self.driver.implicitly_wait(30)

        self.initial_setting()


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
from datetime import datetime, timezone
import time
# from scrapy_selenium import SeleniumRequest
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, NoSuchWindowException, WebDriverException, \
    ElementNotInteractableException
from selenium.webdriver import ActionChains
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

# 뉴스 검색 기간(시작일, 종료일)을 입력받아 해당 뉴스 목록을 db에 저장하는 스파이더.
class NewStockFinancialSpider(scrapy.Spider):
    name = "new_stock_financial_spider";

    def __init__(self):
        super(NewStockFinancialSpider, self).__init__()
        self.driver = None

        # 신규 상장 종목 확인
        self.db = psycopg2.connect(host="112.220.72.179", dbname="openmetric", user="openmetric",
                              password=")!metricAdmin01", port=2345)
        self.cur = self.db.cursor()
        current_stock_list = pd.read_sql("select * from stocks_basic_info where corp_code != ' ' ", self.db).sort_values(by="code")
        last_stock_list = pd.read_excel(constant.error_file_path+"/last_stock_list.xlsx")
        self.new_stock_list = pd.DataFrame()
        # 새로 추가된 종목 리스트 생성.
        for index in current_stock_list.index:
            if (current_stock_list["code"][index] == last_stock_list["code"]).any() == False:
                self.new_stock_list = self.new_stock_list.append(current_stock_list.loc[index])
        # 현재 종목 리스트를 last_stock_list.xlsx로 저장.
        current_stock_list = current_stock_list.drop(["created_at", "updated_at"], axis="columns")
        current_stock_list.to_excel(constant.error_file_path+"/last_stock_list.xlsx", index=False)

    def start_requests(self):
        url_list = [
            "https://www.deepsearch.com/?auth=login"
        ]

        for url in url_list:
            yield scrapy.Request(url=url)

    def parse(self, response):
        try:
            self.initial_setting()
            self.new_stock_financial_scraping()

        except Exception as e:
            date_time = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time()))
            with open(constant.error_file_path + "/big_kinds_news_error_list_" +
                      time.strftime("%Y-%m-%d", time.localtime(time.time())) + ".txt", "a", encoding="UTF-8") as f:
                f.write(date_time + "_전체 에러 \n")
                f.write(traceback.format_exc())

    # 분기별 재무정보 스크래핑 후 업데이트
    def new_stock_financial_scraping(self):
        # 디버깅용
        # self.kospi_list = self.kospi_list[:10]

        # 새 종목 리스트 반복 돌며
        # 엑셀 다운
        # db저장
        #

        # 분기 데이터 받아야 하는 리스트 대상으로 한번 반복.
        item_count = 0 #반복시마다 증가하는 카운트.(크롬 out of memory오류 방지를 위해 체크)
        # 임시 항목 리스트에 대해 분기 데이터 추출.
        for index, company in self.new_stock_list.iterrows():
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
                        "//div[@class='deepsearch-app']/div[contains(@class,'drawer-container-layout')]/"
                        "div[contains(@class,'drawer-container')]/div[contains(@class,'drawer-container-inner')]/"
                        "div[contains(@class,'menu-item-group')][2]/div[contains(@class,'menu-item')][3]")
                    self.driver.execute_script("arguments[0].click();", menu1_button)
                    cm.wait(2)

                    # 통합검색창 기업 단축코드 검색.
                    search_bar = self.driver.find_element_by_xpath(
                        "//div[contains(@class,'deepsearch-appbar')]//div[contains(@class,'search-box')]"
                        "//div[contains(@class,'top-search-conatiner')]//div[contains(@class,'search-bar')]/input"
                    )
                    search_bar.send_keys("")
                    cm.wait(2)
                    search_bar.send_keys(company["code"][1:])
                    cm.wait(2)
                    search_bar.send_keys(Keys.RETURN)

                    # 기업 검색 결과 확인..
                    if self.check_element(
                            "//div[@id='info-list']/div[contains(@class,'search-company-info-view')]", wait_time=30
                    ) == False:
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
                        finance_xpath = "//div[@id='drawer-content-layout']/div[contains(@class,'deepsearch-content')]" \
                                        "//div[contains(@class,'card-layout')][2]/div[contains(@class,'company-statements-details')]"
                        option_xpath = "//div[@id='drawer-content-layout']/div[contains(@class,'deepsearch-content')]" \
                                       "//div[contains(@class,'card-layout')][2]/div[contains(@class,'company-statements-details')]" \
                                       "/div[contains(@class,'header')]/div[contains(@class,'date-option')]"

                        button = self.driver.find_element_by_xpath(option_xpath +
                                                                   "/div[contains(@class,'option')][2]/div[contains(@class,'dropdown-selected')]")
                        self.driver.execute_script("arguments[0].click();", button)
                        cm.wait(2)
                        button = self.driver.find_element_by_xpath(
                            "//div[@id='root']/div[contains(@class,'deepsesarch-dropdown-container')]"
                            "/div[contains(@class,'deepsesarch-dropdown-items')]/div[contains(@class,'items')]"
                            "/div[" + str(i1) + "]"
                        )
                        data_term = button.find_element_by_xpath("./div").text
                        self.driver.execute_script("arguments[0].click();", button)
                        # time.sleep(random.uniform(self.motion_term + 17, self.motion_term + 18))

                        for i2 in range(1, 3):
                            # 연결,개별 선택
                            data_type = self.driver.find_element_by_xpath(option_xpath +
                                                                          "/div[contains(@class,'option')][1]/div[contains(@class,'dropdown-selected')]"
                                                                          "/div[contains(@class,'text')]").text
                            button = self.driver.find_element_by_xpath(option_xpath +
                                                                       "/div[contains(@class,'option')][1]/div[contains(@class,'dropdown-selected')]")
                            self.driver.execute_script("arguments[0].click();", button)
                            cm.wait(2)
                            button = self.driver.find_element_by_xpath(
                                "//div[@id='root']/div[contains(@class,'deepsesarch-dropdown-container')]"
                                "/div[contains(@class,'deepsesarch-dropdown-items')]/div[contains(@class,'items')]"
                                "/div[" + str(i2) + "]"
                            )
                            data_type = button.find_element_by_xpath(
                                "//div[@id='root']/div[contains(@class,'deepsesarch-dropdown-container')]"
                                "/div[contains(@class,'deepsesarch-dropdown-items')]/div[contains(@class,'items')]"
                                "/div[" + str(i2) + "]"
                            ).text
                            self.driver.execute_script("arguments[0].click();", button)

                            # 데이터 로딩 완료 확인.. 1분동안 로딩 안되면 다시 검색?
                            if self.check_element(
                                    finance_xpath + "/div[contains(@class,'fundamental-table-content')]"
                                                    "/div[contains(@class,'react-table-layout')]"
                                                    "/div[contains(@class,'financial-status-table')]"
                                                    "/div[contains(@class,'loading')]",
                                    condition=EC.invisibility_of_element_located,
                                    wait_time=60
                            ) == False:
                                # search_result = False
                                break
                            # 데이터 없는 경우 확인. 없으면 '개별' 재무제표 선택.
                            if self.check_element(
                                    finance_xpath + "/div[contains(@class,'fundamental-table-content')]"
                                                    "/div[contains(@class,'react-table-layout')]"
                                                    "/div[contains(@class,'financial-status-table')]"
                                                    "/div[contains(@class,'rt-noData')]",
                                    condition=EC.invisibility_of_element_located,
                                    # wait_time=5
                            ) == False:
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
                                "//div[@id='drawer-content-layout']/div[contains(@class,'deepsearch-content')]"
                                "//div[contains(@class,'company-header')]/div[contains(@class,'company-info')]"
                                "/div[contains(@class,'name')]"
                            ).text

                            #   포괄손익계산서 다운
                            button = self.driver.find_element_by_xpath(finance_xpath +
                                                                       "/div[contains(@class,'fundamental-table-content')]/div[contains(@class,'react-table-layout')]" \
                                                                       "//span[contains(@class,'table-export-button')]"
                                                                       )
                            self.driver.execute_script("arguments[0].click();", button)
                            cm.wait(5)
                            if os.path.isfile(constant.download_path + "/deepSearch/" + str(company["name"]) + "-포괄손익계산서-" +
                                              data_term + "_" + data_type + ".xlsx"):
                                os.remove(constant.download_path + "/deepSearch/" + str(company["name"]) + "-포괄손익계산서-" +
                                          data_term + "_" + data_type + ".xlsx")
                            os.rename(
                                constant.download_path + "/deepSearch/DeepSearch-포괄손익계산서Table-Export.xlsx",
                                constant.download_path + "/deepSearch/" + str(
                                    company["name"]) + "-포괄손익계산서-" + data_term + "_" + data_type + ".xlsx")

                            #   재무상태표 다운
                            if i1 != 2:
                                button = self.driver.find_element_by_xpath(
                                    finance_xpath + "/div[contains(@class,'type-options')]/div[2]"
                                )
                                self.driver.execute_script("arguments[0].click();", button)
                                cm.wait(5)
                                button = self.driver.find_element_by_xpath(finance_xpath +
                                                                           "/div[contains(@class,'fundamental-table-content')]/div[contains(@class,'react-table-layout')]" \
                                                                           "//span[contains(@class,'table-export-button')]"
                                                                           )
                                self.driver.execute_script("arguments[0].click();", button)
                                cm.wait(5)

                                if os.path.isfile(constant.download_path + "/deepSearch/" + str(
                                        company["name"]) + "-재무상태표-" + data_term + "_" + data_type + ".xlsx"):
                                    os.remove(constant.download_path + "/deepSearch/" + str(
                                        company["name"]) + "-재무상태표-" + data_term + "_" + data_type + ".xlsx")
                                os.rename(constant.download_path + "/deepSearch/DeepSearch-재무상태표Table-Export.xlsx",
                                          constant.download_path + "/deepSearch/" + str(
                                              company["name"]) + "-재무상태표-" + data_term + "_" + data_type + ".xlsx")
                            #   현금흐릅표 다운
                            button = self.driver.find_element_by_xpath(
                                finance_xpath + "/div[contains(@class,'type-options')]/div[3]"
                            )
                            self.driver.execute_script("arguments[0].click();", button)
                            cm.wait(5)
                            button = self.driver.find_element_by_xpath(finance_xpath +
                                                                       "/div[contains(@class,'fundamental-table-content')]/div[contains(@class,'react-table-layout')]" \
                                                                       "//span[contains(@class,'table-export-button')]"
                                                                       )
                            self.driver.execute_script("arguments[0].click();", button)
                            cm.wait(5)
                            if os.path.isfile(constant.download_path + "/deepSearch/" + str(
                                    company["name"]) + "-현금흐름표-" + data_term + "_" + data_type + ".xlsx"):
                                os.remove(constant.download_path + "/deepSearch/" + str(
                                    company["name"]) + "-현금흐름표-" + data_term + "_" + data_type + ".xlsx")
                            os.rename(
                                constant.download_path + "/deepSearch/DeepSearch-현금흐름표Table-Export.xlsx",
                                constant.download_path + "/deepSearch/" + str(
                                    company["name"]) + "-현금흐름표-" + data_term + "_" + data_type + ".xlsx")

                    if quarterly_data_exist == True:
                        # 분기 데이터 엑셀에서 추출하여 저장
                        # 포괄손익계산서
                        pl = pd.read_excel(constant.download_path + "/deepSearch/" + company["name"] + "-포괄손익계산서-분기(3개월)_" + data_type + ".xlsx")
                        # 재무상태표
                        bs = pd.read_excel(constant.download_path + "/deepSearch/" + company["name"] + "-재무상태표-분기(3개월)_" + data_type + ".xlsx")
                        # 현금흐름표
                        cf = pd.read_excel(constant.download_path + "/deepSearch/" + company["name"] + "-현금흐름표-분기(3개월)_" + data_type + ".xlsx")

                        # db에 저장.
                        self.cur.execute("select corp_code from stocks_basic_info where code='" + company["code"] + "' ")
                        corp_code = self.cur.fetchone()[0]

                        insert_sql = ""
                        insert_sql = self.store_quarterly_data(company, pl, "포괄손익계산서", date, corp_code)
                        insert_sql = insert_sql + ", " + self.store_quarterly_data(company, bs, "재무상태표", date, corp_code)
                        insert_sql = insert_sql + ", " + self.store_quarterly_data(company, cf, "현금흐름표", date, corp_code)

                        self.cur.execute("INSERT INTO stock_financial_statement ("
                                         "created_at, updated_at, corp_code, business_year, business_month, this_term_name, "
                                         "subject_name, account_id, account_name, "
                                         "account_level, this_term_amount, ordering, code_id) "
                                         "VALUES " + insert_sql)
                        self.db.commit()

                except NoSuchWindowException as e:
                    self.restart_chrome_driver()

                    # 에러 정보 저장.
                    date_time = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time()))
                    with open(constant.error_file_path + "/quarterly_error_list_" + time.strftime("%Y-%m-%d", time.localtime(time.time())) + ".txt", "a", encoding="UTF-8") as f:
                        f.write(date_time + "_" + company["code"][1:] + "_" + company["name"] + "\n")
                        f.write(traceback.format_exc())
                    continue
                except Exception as e:
                    date_time = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time()))
                    with open(
                            constant.error_file_path + "/quarterly_error_list_" + time.strftime("%Y-%m-%d", time.localtime(time.time())) + ".txt",
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


    def click_element(self, xpath, wait_time):
        button = self.driver.find_element_by_xpath(xpath)
        try:
            button.click()
        except ElementNotInteractableException as e:
            self.driver.execute_script("arguments[0].click();", button)
        # except Exception as e:
        #     self.driver.execute_script("arguments[0].click();", button)
        time.sleep(random.uniform(wait_time, wait_time+1))

    def initial_setting(self):
        for i in range(10):
            try:
                # driver 실행.
                chrome_driver = constant.chrome_driver_path
                chrome_options = Options()
                chrome_options.add_experimental_option("prefs", {
                    "download.default_directory": constant.download_path.replace("/", "\\") + "\\deepSearch",
                    "profile.content_settings.exceptions.automatic_downloads.*.setting": 1,
                    "plugins.always_open_pdf_externally": True
                })
                self.driver = webdriver.Chrome(chrome_driver, chrome_options=chrome_options)
                # self.driver.set_window_position(1300,0)
                # self.driver.set_window_size(1400, 1000)
                self.driver.get('https://www.deepsearch.com/?auth=login')
                self.driver.implicitly_wait(5)
                time.sleep(random.uniform(2, 3))

                # 검색 페이지 세팅.
                # 로그인
                self.driver.find_element_by_xpath(
                    "//div[contains(@class,'login-page')]//div[@class='login-container']//input[@placeholder='계정']").send_keys(
                    "sooryong@gmail.com")
                time.sleep(random.uniform(2 + i, 3 + i))
                self.driver.find_element_by_xpath(
                    "//div[contains(@class,'login-page')]//div[@class='login-container']//input[@placeholder='비밀번호']").send_keys(
                    ")!kaimobile01")
                time.sleep(random.uniform(2 + i, 3 + i))
                self.driver.find_element_by_xpath(
                    "//div[contains(@class,'login-page')]//div[@class='login-container']//input[@class='button login']").click()
                time.sleep(random.uniform(3 + i, 4 + i))
                self.driver.refresh()
                time.sleep(random.uniform(2 + i, 3 + i))

                # 메뉴바 클릭.
                menu_bar_button = self.driver.find_element_by_xpath(
                    "//div[@class='deepsearch-appbar']//div[contains(@class,'app-bar-drawer')]")
                self.driver.execute_script("arguments[0].click();", menu_bar_button)
                time.sleep(random.uniform(2 + i, 3 + i))

                # '기업검색'항목 이동
                menu1_button = self.driver.find_element_by_xpath(
                    "//div[@class='deepsearch-app']/div[contains(@class,'drawer-container-layout')]"
                    "/div[contains(@class,'drawer-container')]/div[contains(@class,'drawer-container-inner')]"
                    "/div[contains(@class,'simplebar-wrapper')]/div[contains(@class,'simplebar-mask')]"
                    "//div[contains(@class,'menu-item-group')][2]/div[contains(@class,'menu-item')][3]")
                self.driver.execute_script("arguments[0].click();", menu1_button)
                time.sleep(random.uniform(2 + i, 3 + i))

            except Exception as e:
                date_time = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time()))
                with open(constant.error_file_path + "/big_kinds_news_error_list_" + time.strftime("%Y-%m-%d", time.localtime(
                            time.time())) + ".txt",
                        "a", encoding="UTF-8") as f:
                    f.write(date_time + "_초기 세팅 실패.\n")
                    f.write(traceback.format_exc())
                self.driver.quit()
                time.sleep(random.uniform(4 + i, 5 + i))
                continue
            else:
                break



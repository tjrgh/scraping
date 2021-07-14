import copy
import os
import random
import re
from shutil import which

import pandas as pd
import psycopg2
import scrapy
import datetime
import time
# from scrapy_selenium import SeleniumRequest
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, NoSuchWindowException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import pymongo

SELENIUM_DRIVER_NAME = 'chrome'
SELENIUM_DRIVER_EXECUTABLE_PATH = which('geckodriver')
SELENIUM_DRIVER_ARGUMENTS=['--headless']  # '--headless' if using chrome instead of firefox

class KoreanDailyFinanceSpider(scrapy.Spider):
    name = "korean_daily_finance_spider";

    def __init__(self):
        super(KoreanDailyFinanceSpider, self).__init__()
        chrome_driver = 'C:/Users/kai/Desktop/chromedriver_win32/chromedriver.exe'
        chrome_options = Options()
        # # chrome_options.add_argument("start-maximized")
        chrome_options.add_experimental_option("prefs", {
            "profile.content_settings.exceptions.automatic_downloads.*.setting": 1
        })
        self.driver = webdriver.Chrome(chrome_driver, chrome_options=chrome_options)

        self.kospi_list = pd.read_excel("C:/Users/kai/Desktop/quarterly_data_list.xlsx")
        self.motion_term = 2

        # self.db = psycopg2.connect(host="112.220.72.178", dbname="openmetric", user="openmetric",
        #                       password=")!metricAdmin01", port=2345)
        # self.cur = self.db.cursor()

    def start_requests(self):
        url_list = [
            "https://www.deepsearch.com/?auth=login"
        ]

        for url in url_list:
            yield scrapy.Request(url=url)

    def parse(self, response):
        self.driver.get(response.url);
        self.driver.implicitly_wait(30);

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
            except Exception as e:
                print(e)
                continue
            else:
                break

        self.quarterly_finance_scraping()

    # 분기별 재무정보 스크래핑 후 업데이트
    def quarterly_finance_scraping(self):
        # 디버깅용
        # self.kospi_list = self.kospi_list[836:]

        # 스크래핑 실패 기록용 파일
        quarterly_complete_list = pd.read_excel("./quarterly_complete_list.xlsx")# 데이터 받은 항목 리스트
        quarterly_error_list = open("./quarterly_error_list.txt", "a", encoding="UTF-8")# 에러 목록

        # 메뉴바 클릭.
        menu_bar_button = self.driver.find_element_by_xpath(
            "//div[@class='deepsearch-appbar']//div[contains(@class,'app-bar-drawer')]")
        self.driver.execute_script("arguments[0].click();", menu_bar_button)
        time.sleep(random.uniform(2, 3))

        # 누적하려는 재무제표 분기 날짜 결정. 현재 날짜 기준으로 결정하면 되겠다.
        today = time.localtime(time.time())
        if today.tm_mon < 4:
            date = str(today.tm_year - 1) + "-12-31"
        elif today.tm_mon < 6:
            date = str(today.tm_year) + "-03-31"
        elif today.tm_mon < 8:
            date = str(today.tm_year) + "-06-30"
        elif today.tm_mon < 12:
            date = str(today.tm_year) + "-09-30"
        #디버깅용
        date = "2021-03-31"

        # 분기 데이터 받아야 하는 리스트 대상으로 한번 반복.
        kospi_list_temp = copy.deepcopy(self.kospi_list)
        item_count = 0 #반복시마다 증가하는 카운트.(크롬 out of memory오류 방지를 위해 체크)
        # 임시 항목 리스트에 대해 분기 데이터 추출.
        for index, company in kospi_list_temp.iterrows():
            search_result = True
            # 실패시 해당 종목을 3번까지 반복.
            for try_count in range(3):
                if search_result == False:
                    break;
                try:
                    # '기업검색'항목 이동
                    menu1_button = self.driver.find_element_by_xpath(
                        "//div[@class='deepsearch-app']/div[contains(@class,'drawer-container-layout')]/"
                        "div[contains(@class,'drawer-container')]/div[contains(@class,'drawer-container-inner')]/"
                        "div[contains(@class,'menu-item-group')][2]/div[contains(@class,'menu-item')][3]")
                    self.driver.execute_script("arguments[0].click();", menu1_button)
                    time.sleep(random.uniform(self.motion_term, self.motion_term + 1))

                    # 단축코드로 기업 검색
                    #   기존 키워드 삭제
                    try:
                        button = self.driver.find_element_by_xpath(
                            "//div[@id='drawer-content-layout']//div[contains(@class,'deepsearch-content')]"
                            "//div[contains(@class,'left-menu-bar')]//div[contains(@class,'filter-list')]"
                            "//div[@class='title' and contains(text(),'관련 키워드')]"
                            "//following::div[contains(@class,'related-keyword-tags')]/div[last()]/span")
                        self.driver.execute_script("arguments[0].click();", button)
                    except (NoSuchElementException):
                        pass
                    #   검색
                    search_input = self.driver.find_element_by_xpath(
                        "//div[@id='drawer-content-layout']//div[contains(@class,'deepsearch-content')]"
                        "//div[contains(@class,'left-menu-bar')]//div[contains(@class,'filter-list')]"
                        "//div[@class='title' and contains(text(),'관련 키워드')]//following::input")
                    search_input.send_keys('')
                    time.sleep(random.uniform(self.motion_term, self.motion_term + 1))
                    search_input.send_keys(company["단축코드"])
                    time.sleep(random.uniform(self.motion_term, self.motion_term + 1))
                    search_input.send_keys(Keys.RETURN)
                    time.sleep(random.uniform(self.motion_term + 15, self.motion_term + 16))

                    try:
                        button = self.driver.find_element_by_xpath(
                            "//div[@id='drawer-content-layout']//div[contains(@class,'deepsearch-content')]"
                            "//div[contains(@class,'company-search-body')]//div[contains(@class,'company-list-component')]"
                            "//div[contains(@class,'company-item')][1]/div[@class='company-name']/a")
                        self.driver.execute_script("arguments[0].click();", button)
                        time.sleep(random.uniform(self.motion_term + 3, self.motion_term + 4))
                    except (NoSuchElementException):
                        search_result = False
                        continue

                    # 재무정보
                    button = self.driver.find_element_by_xpath(
                        "//div[@id='tabs']//a[contains(text(),'재무 정보')]")
                    self.driver.execute_script("arguments[0].click();", button)
                    time.sleep(random.uniform(self.motion_term + 6, self.motion_term + 7))

                    # 포괄손익계산서, 재무상태표, 현금흐름표
                    for i1 in range(3, 4):
                        # 연간,분기 선택
                        button = self.driver.find_element_by_xpath(
                            "//div[@id='income-statement']//div[contains(@class,'header-text')]//div[contains(@class,'options')]/div[1]/div"
                        )
                        self.driver.execute_script("arguments[0].click();", button)
                        # time.sleep(random.uniform(self.motion_term + 10, self.motion_term + 11))
                        time.sleep(random.uniform(self.motion_term, self.motion_term + 1))
                        button = self.driver.find_element_by_xpath(
                            "//div[@id='root']/div/div[contains(@class,'deepsesarch-dropdown-items')]/div[" + str(
                                i1) + "]"
                        )
                        data_term = button.find_element_by_xpath("./div").text
                        self.driver.execute_script("arguments[0].click();", button)
                        # time.sleep(random.uniform(self.motion_term + 17, self.motion_term + 18))
                        time.sleep(random.uniform(self.motion_term, self.motion_term + 1))

                        for i2 in range(1, 2):
                            # 연결,개별 선택
                            button = self.driver.find_element_by_xpath(
                                "//div[@id='income-statement']//div[contains(@class,'header-text')]//div[contains(@class,'options')]"
                                "/div[contains(@class,'option')][2]/div[contains(@class,'dropdown-selected')]"
                            )
                            self.driver.execute_script("arguments[0].click();", button)
                            # time.sleep(random.uniform(self.motion_term + 15, self.motion_term + 16))
                            time.sleep(random.uniform(self.motion_term, self.motion_term + 1))
                            button = self.driver.find_element_by_xpath(
                                "//div[@id='root']/div/div[contains(@class,'deepsesarch-dropdown-items')]/div[" + str(
                                    i2) + "]"
                            )
                            data_type = button.find_element_by_xpath("./div").text
                            self.driver.execute_script("arguments[0].click();", button)
                            time.sleep(random.uniform(self.motion_term + 20, self.motion_term + 21))

                            # 분기데이터 존재하는지 확인.
                            try:
                                self.driver.find_element_by_xpath(
                                    "//div[@id='income-statement']//div[contains(@class,'table-container')]"
                                    "//div[contains(@class,'react-table-layout')]//div[contains(@class,'rt-table')]"
                                    "//div[contains(@class,'rt-thead')]//div[contains(@class,'rt-resizable-header-content') and contains(text(),'" + date + "')]")
                            except NoSuchElementException:
                                search_result = False  # 다음 종목으로 넘어가도록.
                                break

                            #   포괄손익계산서 다운
                            button = self.driver.find_element_by_xpath(
                                "//div[@id='income-statement']//div[contains(@class,'table-container')]"
                                "//span[@class='table-export-button']")
                            self.driver.execute_script("arguments[0].click();", button)
                            time.sleep(random.uniform(self.motion_term + 3, self.motion_term + 4))
                            if os.path.isfile("C:/Users/kai/Downloads/" + str(
                                    company["한글 종목약명"]) + "-포괄손익계산서-" + data_term + "_" + data_type + ".xlsx"):
                                os.remove("C:/Users/kai/Downloads/" + str(
                                    company["한글 종목약명"]) + "-포괄손익계산서-" + data_term + "_" + data_type + ".xlsx")
                            os.rename(
                                "C:/Users/kai/Downloads/DeepSearch-" + company[
                                    "한글 종목약명"] + "-포괄손익계산서Table-Export.xlsx",
                                "C:/Users/kai/Downloads/" + str(
                                    company["한글 종목약명"]) + "-포괄손익계산서-" + data_term + "_" + data_type + ".xlsx")

                            #   재무상태표 다운
                            if i1 != 2:
                                button = self.driver.find_element_by_xpath(
                                    "//div[@id='balance-statements']//div[contains(@class,'table-container')]//span[@class='table-export-button']"
                                )
                                self.driver.execute_script("arguments[0].click();", button)
                                time.sleep(random.uniform(self.motion_term + 3, self.motion_term + 4))

                                if os.path.isfile("C:/Users/kai/Downloads/" + str(
                                        company["한글 종목약명"]) + "-재무상태표-" + data_term + "_" + data_type + ".xlsx"):
                                    os.remove("C:/Users/kai/Downloads/" + str(
                                        company["한글 종목약명"]) + "-재무상태표-" + data_term + "_" + data_type + ".xlsx")
                                os.rename("C:/Users/kai/Downloads/DeepSearch-" + company[
                                    "한글 종목약명"] + "-재무상태표Table-Export.xlsx",
                                          "C:/Users/kai/Downloads/" + str(
                                              company[
                                                  "한글 종목약명"]) + "-재무상태표-" + data_term + "_" + data_type + ".xlsx")
                            #   현금흐릅표 다운
                            button = self.driver.find_element_by_xpath(
                                "//div[@id='cashflow-statements']//div[contains(@class,'table-container')]//span[@class='table-export-button']"
                            )
                            self.driver.execute_script("arguments[0].click();", button)
                            time.sleep(random.uniform(self.motion_term + 3, self.motion_term + 4))
                            if os.path.isfile("C:/Users/kai/Downloads/" + str(
                                    company["한글 종목약명"]) + "-현금흐름표-" + data_term + "_" + data_type + ".xlsx"):
                                os.remove("C:/Users/kai/Downloads/" + str(
                                    company["한글 종목약명"]) + "-현금흐름표-" + data_term + "_" + data_type + ".xlsx")
                            os.rename(
                                "C:/Users/kai/Downloads/DeepSearch-" + company[
                                    "한글 종목약명"] + "-현금흐름표Table-Export.xlsx",
                                "C:/Users/kai/Downloads/" + str(
                                    company["한글 종목약명"]) + "-현금흐름표-" + data_term + "_" + data_type + ".xlsx")

                    # 분기 데이터 엑셀에서 추출하여 저장
                    # 포괄손익계산서
                    pl = pd.read_excel("C:/Users/kai/Downloads/" + company["한글 종목약명"] + "-포괄손익계산서-분기(3개월)_연결.xlsx")
                    # 재무상태표
                    bs = pd.read_excel("C:/Users/kai/Downloads/" + company["한글 종목약명"] + "-재무상태표-분기(3개월)_연결.xlsx")
                    # 현금흐름표
                    cf = pd.read_excel("C:/Users/kai/Downloads/" + company["한글 종목약명"] + "-현금흐름표-분기(3개월)_연결.xlsx")

                    # db에 저장.
                    # self.store_quarterly_data(pl, "포괄손익계산서", date)
                    # self.store_quarterly_data(bs, "재무상태표", date)
                    # self.store_quarterly_data(cf, "현금흐름표", date)


                except NoSuchWindowException as e:
                    self.restart_chrome_driver()

                    # 에러 정보 저장.
                    date_time = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time()))
                    quarterly_error_list.write(
                        date_time + "_" + company["단축코드"] + "_" + company["한글 종목약명"] + "\n")
                    quarterly_error_list.write("Exception : \n")
                    quarterly_error_list.write(str(e) + "\n")
                    continue
                except Exception as e:
                    date_time = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time()))
                    quarterly_error_list.write(
                        date_time + "_" + company["단축코드"] + "_" + company["한글 종목약명"] + "\n")
                    quarterly_error_list.write("Exception : \n")
                    quarterly_error_list.write(str(e) + "\n")
                    continue
                # 성공시 다음 종목 스크래핑 수행.
                else:
                    # 분기 데이터 추출 성공항 종목은 csv파일에서 제외.
                    index = self.kospi_list.loc[(self.kospi_list["단축코드"] == company["단축코드"])].index
                    self.kospi_list = self.kospi_list.drop(index)
                    self.kospi_list.to_excel("C:/Users/kai/Desktop/quarterly_data_list.xlsx")
                    # 성공 목록에 추가.
                    quarterly_complete_list = quarterly_complete_list.append({"단축코드":company["단축코드"], "한글 종목약명":company["한글 종목약명"]}, ignore_index=True)
                    quarterly_complete_list.to_excel("./quarterly_complete_list.xlsx")
                    break;
                finally:
                    item_count = item_count + 1
                    if item_count % 30 == 0:
                        self.restart_chrome_driver()


    def store_quarterly_data(self, df, subject_name, date):
        # dataframe의 컬럼명 변경.
        df.columns = df.loc[0]
        df = df.drop([0])
        for row in df.index:  # 엑셀 파일의 row들에 대해 반복.
            # 컬럼에 대해 반복하여 분기를 나타내는 컬럼인 경우에 data insert.
            self.cur.execute("INSERT INTO stock_financial_statement ("
                        "created_at, updated_at, corp_code, business_year, business_month, this_term_name, subject_name, account_id, account_name, "
                        "account_level, this_term_amount, ordering) "
                        "VALUES ("
                        "'" + str(datetime.now(datetime.timezone.utc)) + "', '" + str(
                datetime.now(datetime.timezone.utc)) + "', "
                                                       "'???', '???', '???', '" + date + "', '"+subject_name+"', '" + str(
                df.loc[row]["account_id"]) + "', "
                                             "'" + df.loc[row]["계정명"] + "', '" + str(
                df.loc[row]["LV"]) + "', '" + str(df.loc[row][date]) + "', '" + str(
                df.loc[row]["LV"]) + "') ")
            self.db.commit()

    def restart_chrome_driver(self):
        self.driver.quit()
        chrome_driver = 'C:/Users/kai/Desktop/chromedriver_win32/chromedriver.exe'
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

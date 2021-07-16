import copy
import os
import random
import traceback

import scrapy
import datetime
import time
# from scrapy_selenium import SeleniumRequest
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, NoSuchWindowException
from selenium.webdriver import ActionChains
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.alert import Alert
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from shutil import which
import pandas as pd
import pymongo

SELENIUM_DRIVER_NAME = 'chrome'
SELENIUM_DRIVER_EXECUTABLE_PATH = which('geckodriver')
SELENIUM_DRIVER_ARGUMENTS=['--headless']  # '--headless' if using chrome instead of firefox

class SampleSpider(scrapy.Spider):

    name = 'noname'

    def __init__(self):
        super(SampleSpider, self).__init__()
        chrome_driver = 'C:/Users/kai/Desktop/chromedriver_win32/chromedriver.exe'
        chrome_options = Options()
        # chrome_options.add_argument("--disable-notifications")
        # chrome_options.add_argument("--disable-infobars")
        # # chrome_options.add_argument("start-maximized")
        # chrome_options.add_argument("--disable-extensions")
        chrome_options.add_experimental_option("prefs", {
            # "profile.default_content_setting_values.notifications": 1
            "profile.content_settings.exceptions.automatic_downloads.*.setting": 1
        })
        self.driver = webdriver.Chrome(chrome_driver, chrome_options=chrome_options)

        self.stock_list = pd.read_excel("C:/Users/kai/Desktop/stock_list.xlsx")
        # client = pymongo.MongoClient('localhost', 27017)
        # self.db = client.noname
        # self.kospi_list = list(self.db.kospi_list.find({}))
        self.motion_term = 2

    def start_requests(self):
        urls = [
            'https://www.deepsearch.com/?auth=login'
            # 'https://www.deepsearch.com/analytics/economic-indicator?pageView=1&symbol=BOK%3A099Y001.305622AA&seriesName=%EC%88%98%EC%B6%9C%EA%B8%88%EC%95%A1%EC%A7%80%EC%88%98%20-%20%ED%99%94%EC%9E%A5%ED%92%88&chartEditSetting=JTdCJTIyc2VyaWVzTGlzdCUyMiUzQSU1QiU3QiUyMnN5bWJvbCUyMiUzQSUyMkJPSyUzQTA5OVkwMDEuMzA1NjIyQUElMjIlMkMlMjJheGlzJTIyJTNBMCU3RCU1RCUyQyUyMmxlZ2VuZFBvc2l0aW9uJTIyJTNBMiUyQyUyMmxlZ2VuZE9yaWVudGF0aW9uJTIyJTNBJTIyaCUyMiU3RA==',
            # 'https://www.deepsearch.com/analytics/economic-indicator?pageView=2&symbol=BOK%3A099Y001.305622AA&seriesName=%EC%88%98%EC%B6%9C%EA%B8%88%EC%95%A1%EC%A7%80%EC%88%98%20-%20%ED%99%94%EC%9E%A5%ED%92%88&chartEditSetting=JTdCJTIyc2VyaWVzTGlzdCUyMiUzQSU1QiU3QiUyMnN5bWJvbCUyMiUzQSUyMkJPSyUzQTA5OVkwMDEuMzA1NjIyQUElMjIlMkMlMjJheGlzJTIyJTNBMCU3RCU1RCUyQyUyMmxlZ2VuZFBvc2l0aW9uJTIyJTNBMiUyQyUyMmxlZ2VuZE9yaWVudGF0aW9uJTIyJTNBJTIyaCUyMiU3RA==',

        ]

        for url in urls:
            # yield SeleniumRequest(url = url, callback=self.parse)
            yield scrapy.Request(url=url)

    def parse(self, response):
        self.driver.get(response.url)
        # WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.XPATH, "//div[@class='login-page huge']//div[@class='ds input input'][position()=1]/input")))
        self.driver.implicitly_wait(30)

        # 로그인
        for i in range(3):
            try:
                time.sleep(random.uniform(3, 4))
                self.driver.find_element_by_xpath("//div[contains(@class,'login-page')]//div[@class='login-container']//input[@placeholder='계정']").send_keys("sooryong@gmail.com")
                time.sleep(random.uniform(3, 4))
                self.driver.find_element_by_xpath("//div[contains(@class,'login-page')]//div[@class='login-container']//input[@placeholder='비밀번호']").send_keys(")!kaimobile01")
                time.sleep(random.uniform(3, 4))
                self.driver.find_element_by_xpath("//div[contains(@class,'login-page')]//div[@class='login-container']//input[@class='button login']").click()
                time.sleep(random.uniform(5, 6))
                self.driver.refresh()
            except Exception as e:
                print(e)
                continue
            else:
                break

        # self.industry_scrap()
        self.company_scrapy()


        return

    def company_scrapy(self):
        #디버깅용
        # self.stock_list = self.stock_list[1523:]

        # 스크래핑 실패 기록용 메모장 파일
        # fail_list_detail_file = open("./fail_list_detail.txt","a", encoding="UTF-8")
        # fail_list_file = open("./fail_list.txt", "a", encoding="UTF-8")
        # error_list_file = open("./error_list_"+time.strftime("%Y-%m-%d", time.localtime(time.time()))+".txt", "a", encoding="UTF-8")

        # 메뉴바 클릭.
        menu_bar_button = self.driver.find_element_by_xpath(
            "//div[@class='deepsearch-appbar']//div[contains(@class,'app-bar-drawer')]")
        self.driver.execute_script("arguments[0].click();", menu_bar_button)
        time.sleep(random.uniform(2, 3))

        a = 0
        while a < 5:
            a += a + 1;
            stock_list_temp = copy.deepcopy(self.stock_list)
            item_count = 0
            for index, company in stock_list_temp.iterrows():
                #파일 3개 다 있으면 패스
                if(
                    os.path.isfile("C:/Users/kai/Downloads/" + str(company["한글 종목약명"]) + "-포괄손익계산서-분기(3개월)_연결.xlsx")
                    and os.path.isfile("C:/Users/kai/Downloads/" + str(company["한글 종목약명"]) + "-재무상태표-분기(3개월)_연결.xlsx")
                    and os.path.isfile("C:/Users/kai/Downloads/" + str(company["한글 종목약명"]) + "-현금흐름표-분기(3개월)_연결.xlsx")
                ):
                    continue

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

                        # 통합검색창 기업 단축코드 검색.
                        search_bar = self.driver.find_element_by_xpath(
                            "//div[contains(@class,'deepsearch-appbar')]//div[contains(@class,'search-box')]"
                            "//div[contains(@class,'top-search-conatiner')]//div[contains(@class,'search-bar')]/input"
                        )
                        search_bar.send_keys("")
                        time.sleep(random.uniform(self.motion_term, self.motion_term + 1))
                        search_bar.send_keys(company["단축코드"])
                        time.sleep(random.uniform(self.motion_term, self.motion_term + 1))
                        search_bar.send_keys(Keys.RETURN)
                        time.sleep(random.uniform(self.motion_term + 7, self.motion_term + 8))

                        # 검색된 기업 클릭
                        try:
                            button = self.driver.find_element_by_xpath(
                                "//div[@id='drawer-content-layout']//div[contains(@class,'deepsearch-content')]"
                                "//div[@id='info-list']//div[contains(@class,'search-company-info-view')]"
                                "/div[contains(@class,'company-info-header')]/a"
                            )
                            self.driver.execute_script("arguments[0].click();", button)
                            time.sleep(random.uniform(self.motion_term + 5, self.motion_term + 6))
                        except NoSuchElementException as e:
                            date_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                            search_result = False
                            with open("./fail_list_detail.txt","a", encoding="UTF-8") as f:
                                f.write(date_time + "_" + company["단축코드"] + "_" + company["한글 종목약명"] + "\n")
                                f.write(traceback.format_exc())
                            with open("./fail_list.txt", "a", encoding="UTF-8") as f:
                                f.write(date_time + "_" + company["단축코드"] + "_" + company["한글 종목약명"] + "\n")
                            continue

                        # # 단축코드로 기업 검색
                        # #   기존 키워드 삭제
                        # try:
                        #     button = self.driver.find_element_by_xpath(
                        #         "//div[@id='drawer-content-layout']//div[contains(@class,'deepsearch-content')]"
                        #         "//div[contains(@class,'left-menu-bar')]//div[contains(@class,'filter-list')]"
                        #         "//div[@class='title' and contains(text(),'관련 키워드')]"
                        #         "//following::div[contains(@class,'related-keyword-tags')]/div[last()]/span")
                        #     self.driver.execute_script("arguments[0].click();", button)
                        # except (NoSuchElementException):
                        #     pass
                        # #   검색
                        # search_input = self.driver.find_element_by_xpath(
                        #     "//div[@id='drawer-content-layout']//div[contains(@class,'deepsearch-content')]"
                        #     "//div[contains(@class,'left-menu-bar')]//div[contains(@class,'filter-list')]"
                        #     "//div[@class='title' and contains(text(),'관련 키워드')]//following::input")
                        # search_input.send_keys('')
                        # time.sleep(random.uniform(self.motion_term, self.motion_term + 1))
                        # search_input.send_keys(company["단축코드"])
                        # time.sleep(random.uniform(self.motion_term, self.motion_term + 1))
                        # search_input.send_keys(Keys.RETURN)
                        # time.sleep(random.uniform(self.motion_term + 15, self.motion_term + 16))
                        #
                        # try:
                        #     button = self.driver.find_element_by_xpath(
                        #         "//div[@id='drawer-content-layout']//div[contains(@class,'deepsearch-content')]"
                        #         "//div[contains(@class,'company-search-body')]//div[contains(@class,'company-list-component')]"
                        #         "//div[contains(@class,'company-item')][1]/div[@class='company-name']/a")
                        #     self.driver.execute_script("arguments[0].click();", button)
                        #     time.sleep(random.uniform(self.motion_term + 3, self.motion_term + 4))
                        # except NoSuchElementException as e:
                        #     date_time = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time()))
                        #     search_result = False
                        #     fail_list_detail_file.write(
                        #         date_time + "_" + company["단축코드"] + "_" + company["한글 종목약명"] + "\n")
                        #     fail_list_file.write(date_time + "_" + company["단축코드"] + "_" + company["한글 종목약명"] + "\n")
                        #     fail_list_detail_file.write("NoSuchElementException : \n")
                        #     fail_list_detail_file.write(str(e) + "\n")
                        #     fail_list_detail_file.write("page source : \n")
                        #     fail_list_detail_file.write(self.driver.page_source + "\n")
                        #     continue

                        # 개요, 현재 정보

                        # 주가, 거래량
                        # self.driver.find_element_by_xpath(
                        #     "//div[@id='tabs']//a[contains(text(),'시장 정보')]").click()
                        # time.sleep(random.uniform(self.motion_term+5, self.motion_term + 6))
                        # button = self.driver.find_element_by_xpath("//div[@id='market-info']//div[contains(@class,'date-options')]"
                        #                                   "/span[contains(text(),'전체')]")
                        # self.driver.execute_script("arguments[0].click();", button)
                        # time.sleep(random.uniform(self.motion_term+4, self.motion_term + 5))
                        # stock_excel_button = self.driver.find_element_by_xpath("//div[@id='market-info']//div[contains(@class,'company-market-container')]"
                        #                                   "//span[contains(@class,'export-button')]")
                        # self.driver.execute_script("arguments[0].click();", stock_excel_button)
                        # time.sleep(random.uniform(self.motion_term+4, self.motion_term + 5))
                        #
                        # os.rename("C:/Users/kai/Downloads/DeepSearch-Chart-Export.xlsx",
                        #           "C:/Users/kai/Downloads/" + str(company["한글 종목명"]) + "-일별_주가_거래량.xlsx")

                        # 재무정보
                        button = self.driver.find_element_by_xpath(
                            "//div[@id='tabs']//a[contains(text(),'재무 정보')]")
                        self.driver.execute_script("arguments[0].click();", button)
                        time.sleep(random.uniform(self.motion_term + 6, self.motion_term + 7))

                        # option_list = self.driver.find_elements_by_xpath("//div[@id='financial-multi']//div[contains(@class,'financial-options')]"
                        #                                                  "//label")
                        # option_list.pop(0)
                        # for option in option_list:
                        #     self.driver.execute_script("arguments[0].click();", option)
                        #     time.sleep(random.uniform(0, 1))
                        # time.sleep(random.uniform(self.motion_term + 3, self.motion_term + 4))
                        # finance_excel_button = self.driver.find_element_by_xpath(
                        #     "//div[@id='financial-multi']//div[contains(@class,'chart-wrapper')]//span[@class='export-button']")
                        # self.driver.execute_script("arguments[0].click();", finance_excel_button)
                        # time.sleep(random.uniform(self.motion_term + 2, self.motion_term + 3))
                        # if os.path.isfile("C:/Users/kai/Downloads/" + str(company["한글 종목약명"]) + "-재무정보.xlsx"):
                        #     os.remove("C:/Users/kai/Downloads/" + str(company["한글 종목약명"]) + "-재무정보.xlsx")
                        # os.rename("C:/Users/kai/Downloads/DeepSearch-Chart-Export.xlsx",
                        #           "C:/Users/kai/Downloads/" + str(company["한글 종목약명"]) + "-재무정보.xlsx")

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
                                    "C:/Users/kai/Downloads/DeepSearch-" + company["한글 종목약명"].replace(" ", "-") + "-포괄손익계산서Table-Export.xlsx",
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
                                        "한글 종목약명"].replace(" ", "-") + "-재무상태표Table-Export.xlsx",
                                              "C:/Users/kai/Downloads/" + str(
                                                  company["한글 종목약명"]) + "-재무상태표-" + data_term + "_" + data_type + ".xlsx")
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
                                    "C:/Users/kai/Downloads/DeepSearch-" + company["한글 종목약명"].replace(" ", "-") + "-현금흐름표Table-Export.xlsx",
                                    "C:/Users/kai/Downloads/" + str(
                                        company["한글 종목약명"]) + "-현금흐름표-" + data_term + "_" + data_type + ".xlsx")

                        # 배당정보
                        # button = self.driver.find_element_by_xpath(
                        #     "//div[@id='tabs']//a[contains(text(),'주주 및 배당 정보')]")
                        # self.driver.execute_script("arguments[0].click();", button)
                        # time.sleep(random.uniform(self.motion_term + 8, self.motion_term + 9))

                        # button = self.driver.find_element_by_xpath(
                        #     "//div[@id='company-dividend']//div[contains(@class,'table-content')]//span[contains(@class,'table-export-button')]"
                        # )
                        # self.driver.execute_script("arguments[0].click();", button)
                        # time.sleep(random.uniform(self.motion_term + 3, self.motion_term + 4))
                        # if os.path.isfile("C:/Users/kai/Downloads/" + str(company["한글 종목약명"]) + "-배당정보.xlsx"):
                        #     os.remove("C:/Users/kai/Downloads/" + str(company["한글 종목약명"]) + "-배당정보.xlsx")
                        # os.rename("C:/Users/kai/Downloads/DeepSearch-" + company["한글 종목약명"] + "-배당-정보Table-Export.xlsx",
                        #           "C:/Users/kai/Downloads/" + str(company["한글 종목약명"]) + "-배당정보.xlsx")

                    except NoSuchElementException as e:
                        # 에러 기록
                        date_time = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time()))
                        with open("./error_list_"+time.strftime("%Y-%m-%d", time.localtime(time.time()))+".txt", "a", encoding="UTF-8") as f:
                            f.write(date_time + "_" + company["단축코드"] + "_" + company["한글 종목약명"] + "\n")
                            f.write(traceback.format_exc())
                        # 3번 모두 실패했으면, 해당 종목을 실패 목록에 기록.
                        if try_count == 2:
                            with open("./fail_list.txt", "a", encoding="UTF-8") as f:
                                f.write(date_time + "_" + company["단축코드"] + "_" + company["한글 종목약명"] + "\n")
                            with open("./fail_list_detail.txt", "a", encoding="UTF-8") as f:
                                f.write(date_time + "_" + company["단축코드"] + "_" + company["한글 종목약명"] + "\n")
                                f.write(traceback.format_exc())
                        continue
                    except NoSuchWindowException as e:
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
                                time.sleep(random.uniform(3, 4))
                                self.driver.find_element_by_xpath(
                                    "//div[contains(@class,'login-page')]//div[@class='login-container']//input[@placeholder='계정']").send_keys(
                                    "sooryong@gmail.com")
                                time.sleep(random.uniform(3, 4))
                                self.driver.find_element_by_xpath(
                                    "//div[contains(@class,'login-page')]//div[@class='login-container']//input[@placeholder='비밀번호']").send_keys(
                                    ")!kaimobile01")
                                time.sleep(random.uniform(3, 4))
                                self.driver.find_element_by_xpath(
                                    "//div[contains(@class,'login-page')]//div[@class='login-container']//input[@class='button login']").click()
                                time.sleep(random.uniform(5, 6))
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
                        # 에러 정보 저장.
                        date_time = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time()))
                        with open("./error_list_" + time.strftime("%Y-%m-%d", time.localtime(time.time())) + ".txt",
                                  "a", encoding="UTF-8") as f:
                            f.write(date_time + "_" + company["단축코드"] + "_" + company["한글 종목약명"] + "\n")
                            f.write(traceback.format_exc())
                        if try_count == 2:
                            with open("./fail_list.txt", "a", encoding="UTF-8") as f:
                                f.write(date_time + "_" + company["단축코드"] + "_" + company["한글 종목약명"] + "\n")
                            with open("./fail_list_detail.txt", "a", encoding="UTF-8") as f:
                                f.write(date_time + "_" + company["단축코드"] + "_" + company["한글 종목약명"] + "\n")
                                f.write(traceback.format_exc())
                        time.sleep(random.uniform(self.motion_term+5, self.motion_term + 6))
                        continue
                    except Exception as e:
                        date_time = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time()))
                        with open("./error_list_" + time.strftime("%Y-%m-%d", time.localtime(time.time())) + ".txt",
                                  "a", encoding="UTF-8") as f:
                            f.write(date_time + "_" + company["단축코드"] + "_" + company["한글 종목약명"] + "\n")
                            f.write(traceback.format_exc())
                        if try_count == 2:
                            with open("./fail_list.txt", "a", encoding="UTF-8") as f:
                                f.write(date_time + "_" + company["단축코드"] + "_" + company["한글 종목약명"] + "\n")
                            with open("./fail_list_detail.txt", "a", encoding="UTF-8") as f:
                                f.write(date_time + "_" + company["단축코드"] + "_" + company["한글 종목약명"] + "\n")
                                f.write(traceback.format_exc())
                        continue
                    # 성공시 다음 종목 스크래핑 수행.
                    else:
                        # self.stock_list.remove(company)
                        index = self.stock_list.loc[(self.stock_list["단축코드"] == company["단축코드"])].index
                        self.stock_list = self.stock_list.drop(index)
                        break;
                    finally:
                        item_count = item_count + 1
                        if item_count % 30 == 0:
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
                                    time.sleep(random.uniform(3, 4))
                                    self.driver.find_element_by_xpath(
                                        "//div[contains(@class,'login-page')]//div[@class='login-container']//input[@placeholder='계정']").send_keys(
                                        "sooryong@gmail.com")
                                    time.sleep(random.uniform(3, 4))
                                    self.driver.find_element_by_xpath(
                                        "//div[contains(@class,'login-page')]//div[@class='login-container']//input[@placeholder='비밀번호']").send_keys(
                                        ")!kaimobile01")
                                    time.sleep(random.uniform(3, 4))
                                    self.driver.find_element_by_xpath(
                                        "//div[contains(@class,'login-page')]//div[@class='login-container']//input[@class='button login']").click()
                                    time.sleep(random.uniform(5, 6))
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




    def industry_scrap(self):
        # 항목 이동
        menu_bar_button = self.driver.find_element_by_xpath("//div[@class='deepsearch-appbar']//div[contains(@class,'app-bar-drawer')]")
        self.driver.execute_script("arguments[0].click();", menu_bar_button)
        time.sleep(random.uniform(2, 3))
        # self.driver.find_element_by_css_selector('div.drawer-container.slider div.menu-item-group:nth-child(2) div.menu-item:nth-child(5)').click()
        menu1_button = self.driver.find_element_by_xpath("//div[@class='deepsearch-app']/div[contains(@class,'drawer-container-layout')]/"
                                                         "div[contains(@class,'drawer-container')]/div[contains(@class,'drawer-container-inner')]/"
                                                         "div[contains(@class,'menu-item-group')][2]/div[@class='menu-item'][2]")
        self.driver.execute_script("arguments[0].click();", menu1_button)
        time.sleep(random.uniform(2, 3))
        # '통계청 표준 산업 분류'선택
        self.driver.find_element_by_xpath(
            "//div[@id='drawer-content-layout']//div[@id='industry_identification_standard']//div[contains(@class,'contents-area')]/div[2]/label").click()

        # 분류 선택
        # 목록 하나씩 추가하며 엑셀 다운.
        for i1 in range(1, 100):
            try:
                #산업분류 - 대분류 선택
                self.driver.find_element_by_xpath(
                    "//div[@id='select_industry']//div[contains(@class,'industry-list-dropdown-wrapper')]"
                    "//div[@class='industry-list-dropdown']/div[1]//div[@class='search']").click()
                time.sleep(random.uniform(2, 3))
                self.driver.find_element_by_xpath(
                    "//div[@id='root']/div[last()]/div[@class='deepsesarch-dropdown-items']/div[" + str(
                        i1) + "]").click()
                time.sleep(random.uniform(2, 3))

                for i2 in range(1, 100):
                    try:
                        # '분석 대상 산업' 목록 비우기.
                        selected_industry_list = self.driver.find_elements_by_xpath(
                            "//div[@id='select_industry']//div[@class='standard-industry-list-view']"
                            "/div[contains(@class,'selected-industry-list')]//ul/li")
                        for i in selected_industry_list:
                            i.find_element_by_xpath(".//div[@class='icon-button']").click()
                            time.sleep(random.uniform(0, 1))
                        # 산업분류 - 중분류 선택
                        self.driver.find_element_by_xpath(
                            "//div[@id='select_industry']//div[contains(@class,'industry-list-dropdown-wrapper')]"
                            "//div[@class='industry-list-dropdown']/div[2]//div[@class='search']").click()
                        time.sleep(random.uniform(2, 3))
                        middle_group = self.driver.find_element_by_xpath(
                            "//div[@id='root']/div[last()]/div[@class='deepsesarch-dropdown-items']/div[" + str(
                                i2) + "]")
                        middle_group.click()
                        time.sleep(random.uniform(2, 3))
                        self.driver.find_element_by_xpath(
                            "//div[@id='select_industry']//div[contains(@class,'industry-list-dropdown-wrapper')]"
                            "//div[@class='contents-area']/div[contains(@class,'button')]").click()
                        time.sleep(random.uniform(5, 6))
                        # 엑셀 다운
                        down_button = self.driver.find_element_by_xpath(
                            "//div[@id='company_list']//div[@class='contents-area']/div[contains(@class,'react-table-layout')]"
                            "/span/span")
                        self.driver.execute_script("arguments[0].click();", down_button)
                        time.sleep(random.uniform(2, 3))

                        os.rename("C:/Users/kai/Downloads/DeepSearch-Table-Export.xlsx",
                                  "C:/Users/kai/Downloads/industry_"+str(i1)+"-"+str(i2)+".xlsx")

                    except(NoSuchElementException):
                        print(NoSuchElementException)
                        break

            except(NoSuchElementException):
                print(NoSuchElementException)
                break


        # mongoDB 실습
        # post = {"author": "Mike",
        #     "text": "My first blog post!",
        #     "tags": ["mongodb", "python", "pymongo"],
        #     "date": datetime.datetime.utcnow()}
        # post_id = self.collection.insert_one(post).inserted_id
        # print(post_id)



class Quote(scrapy.Item):
    sentence = scrapy.Field()
    authorName = scrapy.Field()
    authorLink = scrapy.Field()
    tagList = scrapy.Field()


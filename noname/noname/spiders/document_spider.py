import copy
import os
import random
import re
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
import pymongo

SELENIUM_DRIVER_NAME = 'chrome'
SELENIUM_DRIVER_EXECUTABLE_PATH = which('geckodriver')
SELENIUM_DRIVER_ARGUMENTS=['--headless']  # '--headless' if using chrome instead of firefox

class KoreanDailyFinanceSpider(scrapy.Spider):
    name = "korean_documents_spider";

    def __init__(self):
        super(KoreanDailyFinanceSpider, self).__init__()

        # 크롬 드라이버 생성
        chrome_driver = 'C:/Users/kai/Desktop/chromedriver_win32/chromedriver.exe'
        chrome_options = Options()
        chrome_options.add_experimental_option("prefs", {
            "profile.content_settings.exceptions.automatic_downloads.*.setting": 1
        })
        self.driver = webdriver.Chrome(chrome_driver, chrome_options=chrome_options)

        # 스크래핑 대상인 종목 리스트 로드.
        self.kospi_list = pd.read_excel("C:/Users/kai/Desktop/stock_list.xlsx")
        self.motion_term = 2

        # 스크래핑 데이터 저장할 db연결.
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

        self.document_scraping()

    #
    def document_scraping(self):
        # 디버깅용
        # self.kospi_list = self.kospi_list[836:]

        # 스크래핑 실패 기록용 파일
        # document_complete_list = pd.read_excel("./document_complete_list.xlsx")# 데이터 받은 항목 리스트
        document_error_list = open("./document_error_list"+time.strftime("%Y-%m-%d", time.localtime(time.time()))+".txt", "a", encoding="UTF-8")# 에러 목록
        document_fail_list = pd.read_excel("C:/Users/kai/Desktop/document_fail_list.xlsx")

        # 메뉴바 클릭.
        menu_bar_button = self.driver.find_element_by_xpath(
            "//div[@class='deepsearch-appbar']//div[contains(@class,'app-bar-drawer')]")
        self.driver.execute_script("arguments[0].click();", menu_bar_button)
        time.sleep(random.uniform(2, 3))


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
                    time.sleep(random.uniform(self.motion_term+4, self.motion_term + 5))

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
                    time.sleep(random.uniform(self.motion_term+15, self.motion_term + 16))

                    # 검색된 기업 클릭
                    try:
                        button = self.driver.find_element_by_xpath(
                            "//div[@id='drawer-content-layout']//div[contains(@class,'deepsearch-content')]"
                            "//div[@id='info-list']//div[contains(@class,'search-company-info-view')]"
                            "/div[contains(@class,'company-info-header')]/a"
                        )
                        self.driver.execute_script("arguments[0].click();", button)
                        time.sleep(random.uniform(self.motion_term+5, self.motion_term + 6))
                    except NoSuchElementException as e:
                        date_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                        search_result = False
                        document_fail_list = document_fail_list.append({"단축코드": company["단축코드"], "한글 종목약명": company["한글 종목약명"]}, ignore_index=True)
                        document_fail_list.to_excel("./quarterly_complete_list.xlsx")
                        continue

                    # 문서 저장

                    #   해당 기업의 문서 리스트 가져옴.(제목)
                    stored_document_list = []

                    document_div_xpath = "//div[@id='drawer-content-layout']//div[contains(@class,'deepsearch-content')]"\
                        "//div[contains(@class,'content-wrapper')]//div[@id='documents']"\
                        "//div[contains(@class,'company-document-search')]"

                    #   공시 이동.
                    button = self.driver.find_element_by_xpath(document_div_xpath +
                        "//div[contains(@class,'document-options')]/span[contains(text(),'공시')]"
                    )
                    self.driver.execute_script("arguments[0].click();", button)
                    time.sleep(random.uniform(self.motion_term+4, self.motion_term + 5))

                    #   페이지 순환하며 문서 스크래핑.
                    last_page = self.driver.find_element_by_xpath(document_div_xpath +
                        "//div[contains(@class,'sentiment-document-view')]//div[contains(@class,'document-search-result-view')]"
                        "//div[contains(@class,'result-list')]/div[contains(@class,'page-container')]"
                        "//div[contains(@class,'nav-button')][last()-1]/span"
                    ).text
                    for page_num in range(int(last_page)):
                        document_list = self.driver.find_elements_by_xpath(document_div_xpath +
                            "//div[contains(@class,'sentiment-document-view')]//div[contains(@class,'document-search-result-view')]"
                            "//div[contains(@class,'result-list')]/div[contains(@class,'document-item-view')]"
                        )
                        # 페이지의 문서 목록에 대해 클릭하여 데이터 스크래핑.
                        for document in document_list:
                            # 제목으로 중복인지 확인
                            is_duplicated = False
                            for stored_document in stored_document_list:
                                if stored_document["title"] == document.text:#제목이 같으면 다음 반복.
                                    is_duplicated = True
                                    break
                            if is_duplicated == True:#이미 존재하는 문서이면, 다음 문서 반복.
                                continue

                            for retry_count in range(3):
                                try:
                                    # 문서 데이터 추출.
                                    document_title = document.find_element_by_xpath(
                                        "./div[contains(@class,'doc-title')]/span"
                                    ).text # 제목 추출.
                                    document_date = document.find_element_by_xpath(
                                        "./div[contains(@class,'doc-metadata')]/span[3]"
                                    ).text # 날짜 추출.
                                    # url 추출.
                                    button = document.find_element_by_xpath("./div[contains(@class,'doc-title')]/span")
                                    self.driver.execute_script("arguments[0].click();", button)#문서 클릭. 새 탭에서 파일 뜸.
                                    time.sleep(random.uniform(self.motion_term+2, self.motion_term + 3))
                                    self.driver.switch_to.window(self.driver.window_handles[1])#탭 전환
                                    document_url = self.driver.current_url#url 추출
                                    #추가 작업 ~~~~~ 제목, 시간,



                                    self.driver.close()#탭 닫기
                                    time.sleep(random.uniform(self.motion_term, self.motion_term + 1))
                                    self.driver.switch_to.window(self.driver.window_handles[0])#탭 돌아옴.
                                except Exception as e: #문서데이터 추출 중 예외 발생시
                                    date_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                                    document_error_list.write(date_time + "_" + company["단축코드"] + "_" + company["한글 종목약명"] + "\n")
                                    document_error_list.write(traceback.format_exc())
                                    continue
                                else:
                                    break

                        # 다음 페이지 이동
                        button = self.driver.find_element_by_xpath(document_div_xpath +
                            "//div[contains(@class,'sentiment-document-view')]//div[contains(@class,'document-search-result-view')]"
                            "//div[contains(@class,'result-list')]/div[contains(@class,'page-container')]"
                            "//div[contains(@class,'nav-button')][last()]"
                        )
                        if "disable" in button.value_of_css_property("class"):#마지막 페이지 이면 다음 종목 반복
                            break
                        else:
                            self.driver.execute_script("arguments[0].click();", button)#다음 페이지 클릭
                            time.sleep(random.uniform(self.motion_term + 3, self.motion_term + 4))

                except NoSuchWindowException as e:
                    self.restart_chrome_driver()

                    # 에러 정보 저장.
                    date_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                    document_error_list.write(date_time + "_" + company["단축코드"] + "_" + company["한글 종목약명"] + "\n")
                    document_error_list.write(traceback.format_exc())
                    if try_count == 2:
                        document_fail_list = document_fail_list.append( {"단축코드": company["단축코드"], "한글 종목약명": company["한글 종목약명"]}, ignore_index=True)
                        document_fail_list.to_excel("./quarterly_complete_list.xlsx")
                    continue
                except Exception as e:
                    date_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                    document_error_list.write(date_time + "_" + company["단축코드"] + "_" + company["한글 종목약명"] + "\n")
                    document_error_list.write(traceback.format_exc())
                    if try_count == 2:
                        document_fail_list = document_fail_list.append( {"단축코드": company["단축코드"], "한글 종목약명": company["한글 종목약명"]}, ignore_index=True)
                        document_fail_list.to_excel("./quarterly_complete_list.xlsx")
                    continue
                else:# 성공시 다음 종목 스크래핑 수행.
                    # 분기 데이터 추출 성공항 종목은 csv파일에서 제외.
                    index = self.kospi_list.loc[(self.kospi_list["단축코드"] == company["단축코드"])].index
                    self.kospi_list = self.kospi_list.drop(index)
                    self.kospi_list.to_excel("C:/Users/kai/Desktop/quarterly_data_list.xlsx")
                    # 성공 목록에 추가.
                    quarterly_complete_list = quarterly_complete_list.append({"단축코드":company["단축코드"], "한글 종목약명":company["한글 종목약명"]}, ignore_index=True)
                    quarterly_complete_list.to_excel("./quarterly_complete_list.xlsx")
                    break;
                finally:
                    # 종목 30개 스크래핑마다 크롬창을 닫았다 새로 열어줌.
                    item_count = item_count + 1
                    if item_count % 30 == 0:
                        self.restart_chrome_driver()


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

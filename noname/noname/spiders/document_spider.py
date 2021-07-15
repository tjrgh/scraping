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
            "download.default_directory": "C:\\Users\\kai\\Desktop\\korean_stock_document_list\\temp_document",
            "profile.content_settings.exceptions.automatic_downloads.*.setting": 1,
            "plugins.always_open_pdf_externally": True
        })
        self.driver = webdriver.Chrome(chrome_driver, chrome_options=chrome_options)

        # 스크래핑 대상인 종목 리스트 로드.
        self.stock_list = pd.read_excel("C:/Users/kai/Desktop/stock_list.xlsx")
        self.motion_term = 2

        self.document_fail_list = pd.read_excel("C:/Users/kai/Desktop/korean_stock_document_list/document_fail_list.xlsx")

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
        self.stock_list = self.stock_list[1466:]

        # 스크래핑 실패 기록용 파일
        # document_complete_list = pd.read_excel("./document_complete_list.xlsx")# 데이터 받은 항목 리스트
        # document_error_list = open("./document_error_list_"+time.strftime("%Y-%m-%d", time.localtime(time.time()))+".txt", "a", encoding="UTF-8")# 에러 목록


        # 메뉴바 클릭.
        menu_bar_button = self.driver.find_element_by_xpath(
            "//div[@class='deepsearch-appbar']//div[contains(@class,'app-bar-drawer')]")
        self.driver.execute_script("arguments[0].click();", menu_bar_button)
        time.sleep(random.uniform(2, 3))

        a = 0
        # 종목 목록에서 성공한
        while a < 5:
            a += a + 1;
            stock_list_temp = copy.deepcopy(self.stock_list)
            item_count = 0 #반복시마다 증가하는 카운트.(크롬 out of memory오류 방지를 위해 체크)
            # 임시 항목 리스트에 대해 분기 데이터 추출.
            for index, company in stock_list_temp.iterrows():
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
                            self.report_error(company)
                            search_result = False
                            # 실패 목록 기록
                            self.report_fail_list(company)
                            continue

                        # 문서 저장
                        # 해당 종목의 폴더 생성.
                        folder_path = "C:/Users/kai/Desktop/korean_stock_document_list/list/"+company["단축코드"]+"_"+company["한글 종목약명"]
                        if os.path.isdir(folder_path) == True:
                                # 있으면 폴더 삭제 후 생성.(공시 문서의 제목이 같은 것들이 있고 문서를 구분할 값이 따로 존재하지 않아,
                                # 중간부터 다시 받게 될 경우, 겹치는 문서를 가려낼 수 없다. 해서 삭제하고 처음부터 다시 받음.
                            shutil.rmtree(folder_path, ignore_errors=True)
                        os.makedirs(folder_path, exist_ok=True)
                        os.makedirs(folder_path + "/report", exist_ok=True)
                        os.makedirs(folder_path + "/notice", exist_ok=True)

                        #   해당 기업의 문서 리스트 가져옴.(제목)
                        stored_document_list = []

                        document_div_xpath = "//div[@id='drawer-content-layout']//div[contains(@class,'deepsearch-content')]"\
                            "//div[contains(@class,'content-wrapper')]//div[@id='documents']"\
                            "//div[contains(@class,'company-document-search')]"

                        # 리포트 이동.
                        button = self.driver.find_element_by_xpath(document_div_xpath +
                            "//div[contains(@class,'document-options')]/span[contains(text(),'증권사리포트')]"
                        )
                        self.driver.execute_script("arguments[0].click();", button)
                        time.sleep(random.uniform(self.motion_term + 4, self.motion_term + 5))

                        #   페이지 순환하며 문서 스크래핑.
                        last_page_button = self.driver.find_element_by_xpath(document_div_xpath +
                             "//div[contains(@class,'sentiment-document-view')]//div[contains(@class,'document-search-result-view')]"
                             "//div[contains(@class,'result-list')]/div[contains(@class,'page-container')]"
                             "//div[contains(@class,'nav-button')][last()-1]/span"
                         )
                        self.driver.execute_script("arguments[0].click();", last_page_button)  # 마지막 페이지로 이동.
                        time.sleep(random.uniform(self.motion_term + 4, self.motion_term + 5))
                        last_page = last_page_button.text
                        document_count = 0
                        for page_num in range(int(last_page)):
                            document_list = self.driver.find_elements_by_xpath(document_div_xpath +
                               "//div[contains(@class,'sentiment-document-view')]//div[contains(@class,'document-search-result-view')]"
                               "//div[contains(@class,'result-list')]/div[contains(@class,'document-item-view')]"
                           )
                            document_list.reverse()
                            # 페이지의 문서 목록에 대해 클릭하여 데이터 스크래핑.
                            for document in document_list:
                                # 제목으로 중복인지 확인
                                is_duplicated = False
                                for stored_document in stored_document_list:
                                    if stored_document["title"] == document.text:  # 제목이 같으면 다음 반복.
                                        is_duplicated = True
                                        break
                                if is_duplicated == True:  # 이미 존재하는 문서이면, 다음 문서 반복.
                                    continue

                                for retry_count in range(3):
                                    try:
                                        # 문서 데이터 추출.
                                        document_title = document.find_element_by_xpath(
                                            "./div[contains(@class,'doc-title')]/span"
                                        ).text  # 제목 추출.
                                        broker = document.find_element_by_xpath(
                                            "./div[contains(@class,'doc-metadata')]/span[2]"
                                        ).text # 증권사 명 추출.
                                        document_date = document.find_element_by_xpath(
                                            "./div[contains(@class,'doc-metadata')]/span[4]"
                                        ).text  # 날짜 추출.
                                        # url 추출.
                                        button = document.find_element_by_xpath(
                                            "./div[contains(@class,'doc-title')]/span")
                                        self.driver.execute_script("arguments[0].click();", button)  # 문서 제목 클릭. 새 탭에서 파일 뜸.
                                        time.sleep(random.uniform(self.motion_term + 2, self.motion_term + 3))

                                        document_file = os.listdir("C:/Users/kai/Desktop/korean_stock_document_list/temp_document")
                                        stored_document_file_name = document_title
                                        for c in "/.,[]{}();:\"\'*?\\<> ": # 폴더명에 특수문자 제거.
                                            stored_document_file_name = stored_document_file_name.replace(c, "_")
                                        os.rename(
                                            "C:/Users/kai/Desktop/korean_stock_document_list/temp_document/" + document_file[0],
                                            folder_path + "/report/" + format(document_count + 1, "04") + "_" +
                                            stored_document_file_name+os.path.splitext(document_file[0])[1])
                                        document_count = document_count + 1

                                        # db저장


                                    except Exception as e:  # 문서데이터 추출 중 예외 발생시
                                        # 딥서치 기업 상세 창을 제외하고 나머지 창(문서 상세창, 다운창) 닫아줌
                                        for i in range(len(self.driver.window_handles) - 1):
                                            self.driver.switch_to.window(self.driver.window_handles[1])
                                            self.driver.close()
                                        self.driver.switch_to.window(self.driver.window_handles[0])

                                        date_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                                        with open("./document_error_list_" + time.strftime("%Y-%m-%d", time.localtime(
                                                time.time())) + ".txt", "a", encoding="UTF-8") as f:
                                            f.write(date_time + "_" + company["단축코드"] + "_" + company["한글 종목약명"] + "_" +
                                                    document_title + "\n")
                                            f.write(traceback.format_exc())
                                        if retry_count == 2:
                                            # 실패 목록 기록
                                            document_fail_list = document_fail_list.append(
                                                {"단축코드": company["단축코드"], "한글 종목약명": company["한글 종목약명"],
                                                 "문서명": document_title}, ignore_index=True)
                                            document_fail_list.to_excel("C:/Users/kai/Desktop/korean_stock_document_list/document_fail_list.xlsx")
                                        continue
                                    else:
                                        break

                            # 다음 페이지 이동
                            button = self.driver.find_element_by_xpath(document_div_xpath +
                                                                       "//div[contains(@class,'sentiment-document-view')]//div[contains(@class,'document-search-result-view')]"
                                                                       "//div[contains(@class,'result-list')]/div[contains(@class,'page-container')]"
                                                                       "//div[contains(@class,'nav-button')][1]"
                                                                       )
                            if "disable" in button.value_of_css_property("class"):  # 마지막 페이지 이면 다음 종목 반복
                                break
                            else:
                                self.driver.execute_script("arguments[0].click();", button)  # 다음 페이지 클릭
                                time.sleep(random.uniform(self.motion_term + 3, self.motion_term + 4))



                        #   공시 이동.
                        button = self.driver.find_element_by_xpath(document_div_xpath +
                            "//div[contains(@class,'document-options')]/span[contains(text(),'공시')]"
                        )
                        self.driver.execute_script("arguments[0].click();", button)
                        time.sleep(random.uniform(self.motion_term+4, self.motion_term + 5))

                        #   페이지 순환하며 문서 스크래핑.
                        last_page_button = self.driver.find_element_by_xpath(document_div_xpath +
                            "//div[contains(@class,'sentiment-document-view')]//div[contains(@class,'document-search-result-view')]"
                            "//div[contains(@class,'result-list')]/div[contains(@class,'page-container')]"
                            "//div[contains(@class,'nav-button')][last()-1]/span"
                        )
                        self.driver.execute_script("arguments[0].click();", last_page_button)#마지막 페이지로 이동.
                        time.sleep(random.uniform(self.motion_term + 4, self.motion_term + 5))
                        last_page = last_page_button.text
                        document_count = 0
                        for page_num in range(int(last_page)):
                            document_list = self.driver.find_elements_by_xpath(document_div_xpath +
                                "//div[contains(@class,'sentiment-document-view')]//div[contains(@class,'document-search-result-view')]"
                                "//div[contains(@class,'result-list')]/div[contains(@class,'document-item-view')]"
                            )
                            document_list.reverse()
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
                                        # 파일 다운
                                        button = self.driver.find_element_by_xpath(
                                            "//div[@id='north']//div[contains(@class,'view_search')]/ul/li"
                                            "//img[contains(@title,'다운로드')]"
                                        )
                                        self.driver.execute_script("arguments[0].click();", button)
                                        time.sleep(random.uniform(self.motion_term+1, self.motion_term + 2))
                                        self.driver.switch_to.window(self.driver.window_handles[2])  # 탭 전환
                                        document_file_list = self.driver.find_elements_by_xpath(
                                            "//div[contains(@class,'popFile')]//tr"
                                        )
                                        del document_file_list[0]
                                        for document_file in document_file_list:
                                            file_name = document_file.find_element_by_xpath("./td[1]").text
                                            button = document_file.find_element_by_xpath("./td[2]/a")
                                            self.driver.execute_script("arguments[0].click();", button)
                                            time.sleep(random.uniform(self.motion_term+1, self.motion_term + 2))

                                            os.rename("C:/Users/kai/Desktop/korean_stock_document_list/temp_document/"+file_name,
                                                      folder_path+"/notice/"+format(document_count+1, "04")+"_"+file_name)
                                            document_count = document_count + 1
                                            # shutil.move("C:/Users/kai/Downloads/"+file_name, folder_path+"/"+file_name)
                                        self.driver.close()
                                        time.sleep(random.uniform(self.motion_term, self.motion_term + 1))
                                        self.driver.switch_to.window(self.driver.window_handles[1])  # 탭 돌아옴.
                                        self.driver.close()#탭 닫기
                                        time.sleep(random.uniform(self.motion_term, self.motion_term + 1))
                                        self.driver.switch_to.window(self.driver.window_handles[0])#탭 돌아옴.

                                        # db저장


                                    except Exception as e: #문서데이터 추출 중 예외 발생시
                                        # 딥서치 기업 상세 창을 제외하고 나머지 창(문서 상세창, 다운창) 닫아줌
                                        for i in range(len(self.driver.window_handles)-1):
                                            self.driver.switch_to.window(self.driver.window_handles[1])
                                            self.driver.close()
                                        self.driver.switch_to.window(self.driver.window_handles[0])

                                        date_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                                        with open("./document_error_list_"+time.strftime("%Y-%m-%d", time.localtime(time.time()))+".txt", "a", encoding="UTF-8") as f:
                                            f.write(date_time + "_" + company["단축코드"] + "_" + company["한글 종목약명"] + "_" +
                                            document_title+"\n")
                                            f.write(traceback.format_exc())
                                        if retry_count == 2:
                                            # 실패 목록 기록
                                            document_fail_list = document_fail_list.append(
                                                {"단축코드": company["단축코드"], "한글 종목약명": company["한글 종목약명"],
                                                 "문서명": document_title}, ignore_index=True)
                                            document_fail_list.to_excel("C:/Users/kai/Desktop/korean_stock_document_list/document_fail_list.xlsx")
                                        continue
                                    else:
                                        break

                            # 다음 페이지 이동
                            button = self.driver.find_element_by_xpath(document_div_xpath +
                                "//div[contains(@class,'sentiment-document-view')]//div[contains(@class,'document-search-result-view')]"
                                "//div[contains(@class,'result-list')]/div[contains(@class,'page-container')]"
                                "//div[contains(@class,'nav-button')][1]"
                            )
                            if "disable" in button.value_of_css_property("class"):#마지막 페이지 이면 다음 종목 반복
                                break
                            else:
                                self.driver.execute_script("arguments[0].click();", button)#다음 페이지 클릭
                                time.sleep(random.uniform(self.motion_term + 3, self.motion_term + 4))

                    except NoSuchWindowException as e:
                        self.restart_chrome_driver()

                        # 에러 정보 저장.
                        self.report_error(company)
                        if try_count == 2:
                            self.report_fail_list(company)
                        continue
                    except Exception as e:
                        self.report_error(company)
                        if try_count == 2:
                            self.report_fail_list(company)
                        continue
                    else:# 성공시 다음 종목 스크래핑 수행.
                        # 분기 데이터 추출 성공항 종목은 파일에서 제외.
                        index = self.stock_list.loc[(self.stock_list["단축코드"] == company["단축코드"])].index
                        self.stock_list = self.stock_list.drop(index)
                        # 성공 목록에 추가.
                        # document_complete_list = document_complete_list.append({"단축코드":company["단축코드"], "한글 종목약명":company["한글 종목약명"]}, ignore_index=True)
                        # document_complete_list.to_excel("C:/Users/kai/Desktop/document_complete_list.xlsx")
                        break;
                    finally:
                        # 종목 30개 스크래핑마다 크롬창을 닫았다 새로 열어줌.
                        item_count = item_count + 1
                        if item_count % 30 == 0:
                            self.restart_chrome_driver()

    def report_error(self, company):
        date_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
        with open(
                "./document_error_list_" + time.strftime("%Y-%m-%d", time.localtime(time.time())) + ".txt",
                "a", encoding="UTF-8") as f:
            f.write(date_time + "_" + company["단축코드"] + "_" + company["한글 종목약명"] + "\n")
            f.write(traceback.format_exc())
    def report_fail_list(self, company):
        self.document_fail_list = self.document_fail_list.append({"단축코드": company["단축코드"], "한글 종목약명": company["한글 종목약명"]},
                                                       ignore_index=True)
        self.document_fail_list.to_excel("C:/Users/kai/Desktop/korean_stock_document_list/document_fail_list.xlsx")

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

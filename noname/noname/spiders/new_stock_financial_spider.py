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

        for index in current_stock_list.index:
            if (current_stock_list["code"][index] == last_stock_list["code"]).any() == False:
                self.new_stock_list = self.new_stock_list.append(current_stock_list.loc[index])

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

        # 분기 데이터 받아야 하는 리스트 대상으로 한번 반복.
        item_count = 0 #반복시마다 증가하는 카운트.(크롬 out of memory오류 방지를 위해 체크)
        # 임시 항목 리스트에 대해 분기 데이터 추출.
        for index, company in self.new_stock_list.iterrows():
            search_result = True
            # 실패시 해당 종목을 3번까지 반복.
            for try_count in range(3):
                if search_result == False:
                    break;

                # db에 이미 해당 데이터가 있는지 확인.
                #   정확하게 들어 갔는지에 대한 여부가 아닌, 데이터 insert를 이미 한 항목인지 아닌지 여부이다.
                #   스크래핑 속도를 위해서 체크하는 것이므로, 불완전하지만 해당 기간 내의 row가 하나라도 있으면 이미
                #   스크래핑을 완료한 종목으로 취급. 데이터가 없어서 row가 없는건 배제. 검색어를 변경하여 업데이트 하려는 경우
                #   이 중복 부분을 주석 처리 후 스크래핑 실행하면 되겠고, db insert부분에서는 항상 기존에 해당 종목, 기간 내의
                #   데이터가 있을 시 삭제하는 로직을 추가한다.
                try:
                    self.cur.execute(
                        "select news.id from stocks_news as news where code_id='"+company["code"]+"'"+\
                        "   and (news.date <= '"+self.end_date+"' and news.date >= '"+self.start_date+"') "
                    )
                    pre_news_list = self.cur.fetchone()

                    if (pre_news_list != None):
                        break
                except Exception as e:
                    date_time = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time()))
                    with open(constant.error_file_path + "/big_kinds_news_error_list_" +
                              time.strftime("%Y-%m-%d", time.localtime(time.time())) + ".txt", "a",
                              encoding="UTF-8") as f:
                        f.write(date_time + "_" + company["code"][1:] + "_" + company["name"] + "\n")
                        f.write(traceback.format_exc())
                    continue

                try:
                    # 분석 단어, 조건 입력 및 검색
                    temp_button = self.driver.find_element_by_xpath("//button[@id='collapse-step-1']")
                    if "open" not in temp_button.get_attribute("class"):
                        self.click_element("//button[@id='collapse-step-1']", 2)
                    self.driver.find_element_by_xpath("//input[@id='total-search-key']").clear()
                    time.sleep(random.uniform(2,3))
                    self.driver.find_element_by_xpath("//input[@id='total-search-key']").send_keys(company["name"])
                    time.sleep(random.uniform(1, 2))
                    self.driver.find_element_by_xpath("//input[@id='total-search-key']").send_keys(Keys.ARROW_UP+Keys.ARROW_UP)
                    time.sleep(random.uniform(1, 2))
                    temp_button = self.driver.find_element_by_xpath(
                        "//div[@id='collapse-step-1-body']//div[contains(@class,'srch-detail')]"
                        "//div[contains(@class,'tab-btn-wp1')]"
                        "//a[contains(@class,'tab-btn') and contains(text(),'기간')]"
                    )
                    if temp_button.get_attribute("title") == "Open":
                        self.click_element(
                            "//div[@id='collapse-step-1-body']//div[contains(@class,'srch-detail')]"
                            "//div[contains(@class,'tab-btn-wp1')]"
                            "//a[contains(@class,'tab-btn') and contains(text(),'기간')]", 2
                        )

                    temp_button = self.driver.find_element_by_xpath("//input[@id='search-begin-date']")
                    if self.start_date != temp_button.get_attribute("value"):
                        for i in range(10):
                            self.driver.find_element_by_xpath("//input[@id='search-begin-date']").send_keys(Keys.BACK_SPACE)
                            time.sleep(random.uniform(0,0.2))
                        time.sleep(random.uniform(2,3))
                        self.driver.find_element_by_xpath("//input[@id='search-begin-date']").send_keys(self.start_date)
                        time.sleep(random.uniform(2, 3))

                    temp_button = self.driver.find_element_by_xpath("//input[@id='search-end-date']")
                    if self.end_date != temp_button.get_attribute("value"):
                        for i in range(10):
                            self.driver.find_element_by_xpath("//input[@id='search-end-date']").send_keys(Keys.BACK_SPACE)
                            time.sleep(random.uniform(0, 0.2))
                        time.sleep(random.uniform(2, 3))
                        self.driver.find_element_by_xpath("//input[@id='search-end-date']").send_keys(self.end_date)
                        time.sleep(random.uniform(2, 3))

                    # 동의어, 포함어, 제외어 입력.
                    if (self.keyword_list["code_id"]==company["code"]).any():
                        or_include_keyword_list = \
                        self.keyword_list[self.keyword_list["code_id"] == company["code"]]["or_include_keyword_list"].iloc[0]
                        and_include_keyword_list = \
                        self.keyword_list[self.keyword_list["code_id"] == company["code"]]["and_include_keyword_list"].iloc[0]
                        exclude_keyword_list = self.keyword_list[self.keyword_list["code_id"] == company["code"]][
                            "exclude_keyword_list"].iloc[0]

                        temp_button = self.driver.find_element_by_xpath(
                            "//div[@id='collapse-step-1-body']//div[contains(@class,'srch-detail')]"
                            "//div[contains(@class,'tab-btn-wp3')]"
                            "//a[contains(@class,'tab-btn') and contains(text(),'상세검색')]"
                        )
                        if temp_button.get_attribute("title") == "Open":
                            self.click_element(
                                "//div[@id='collapse-step-1-body']//div[contains(@class,'srch-detail')]"
                                "//div[contains(@class,'tab-btn-wp3')]"
                                "//a[contains(@class,'tab-btn') and contains(text(),'상세검색')]", 2
                            )

                        or_include_keyword = ""
                        for keyword in or_include_keyword_list.split("\\"):
                            or_include_keyword = or_include_keyword+", "+keyword
                        self.driver.find_element_by_xpath("//input[@id='orKeyword1']").send_keys(or_include_keyword[2:])
                        time.sleep(random.uniform(2, 3))

                        and_include_keyword = ""
                        for keyword in and_include_keyword_list.split("\\"):
                            and_include_keyword = and_include_keyword+", "+keyword
                        self.driver.find_element_by_xpath("//input[@id='andKeyword1']").send_keys(and_include_keyword[2:])
                        time.sleep(random.uniform(2, 3))

                        exclude_keyword = ""
                        for keyword in exclude_keyword_list.split("\\"):
                            exclude_keyword = exclude_keyword+", "+keyword
                        self.driver.find_element_by_xpath("//input[@id='notKeyword1']").send_keys(exclude_keyword[2:])
                        time.sleep(random.uniform(2, 3))

                        self.click_element(
                            "//div[@id='detailSrch1']//div[contains(@class,'srch-foot')]"
                            "//button[contains(@class,'news-search-btn')]", 2
                        )

                    else:
                        self.click_element(
                            "//div[@id='search-foot-div']/div[contains(@class,'foot-btn')]"
                            "/button[contains(@class,'news-search-btn')]",3
                        )

                    # 다운 받을 파일과 같은 이름의 파일 있을시 삭제.
                    if os.path.isfile(constant.download_path+"/bigkinds/NewsResult_"+self.start_date.replace("-","")+
                        "-"+self.end_date.replace("-","")+".xlsx"):
                        os.remove(constant.download_path+"/bigkinds/NewsResult_"+self.start_date.replace("-","")+
                        "-"+self.end_date.replace("-","")+".xlsx")
                    # 엑셀 다운 탭 클릭.
                    self.click_element("//button[@id='collapse-step-3']",2)

                    # 데이터 로딩 체크
                    WebDriverWait(self.driver, 60).until(
                        EC.invisibility_of_element_located((By.XPATH,
                            "//div[@id='analytics-data-download']/div[contains(@class,'data-down-scroll')]"
                            "//div[contains(@class,'news-loader')]"
                        ))
                    )
                    time.sleep(random.uniform(3, 4))
                    # 다운 버튼 클릭.
                    self.click_element(
                        "//div[@id='analytics-data-download']/div[contains(@class,'btm-btn-wrp')]"
                        "/button[contains(@class,'news-download-btn')]", 2
                    )
                    self.driver.switch_to.alert.accept()
                    time.sleep(random.uniform(2, 3))

                    # 다운 완료 체크
                    for i in range(60):
                        if os.path.isfile(constant.download_path+"/bigkinds/NewsResult_"+self.start_date.replace("-","")+
                        "-"+self.end_date.replace("-","")+".xlsx"):
                            break;
                        else:
                            time.sleep(1)
                            if i == 59:
                                raise Exception("엑셀 파일 다운 대기 시간 초과.")
                    time.sleep(random.uniform(5, 6))

                    if os.path.isfile(constant.download_path+"/bigkinds/"+company["name"]+"_"
                                      +self.start_date+"_"+self.end_date+".xlsx"):
                        os.remove(constant.download_path+"/bigkinds/"+company["name"]+"_"
                                      +self.start_date+"_"+self.end_date+".xlsx")
                    os.rename(
                        constant.download_path+"/bigkinds/NewsResult_"+self.start_date.replace("-","")+
                        "-"+self.end_date.replace("-","")+".xlsx",
                        constant.download_path+"/bigkinds/"+company["name"]+"_"+self.start_date+"_"+self.end_date+".xlsx"
                    )

                    # 기존 db 데이터 삭제
                    self.cur.execute("delete from stocks_news news "
                                     "where news.code_id='"+company["code"]+"' and news.date<='"+self.end_date+"' "
                                     "and news.date>='"+self.start_date+"' ")
                    self.db.commit()

                    # db 저장.
                    news_list = pd.read_excel(constant.download_path+"/bigkinds/"+company["name"]+"_"+
                                              self.start_date+"_"+self.end_date+".xlsx")
                    if news_list.empty == False:
                        insert_sql = ""
                        for index in news_list.index:
                            # 날짜 처리
                            date = str(news_list["일자"][index])
                            date = date[0:4]+"-"+date[4:6]+"-"+date[6:]
                            # 제목 유효성 검증
                            if len(news_list["제목"][index]) >= 100:
                                news_list["제목"][index] = news_list["제목"][index][:96]+"..."
                            title = news_list["제목"][index].replace("'", "`")

                            # url 처리
                            if pd.isna(news_list["URL"][index]):
                                url = ""
                            else:
                                url = news_list["URL"][index]

                            insert_sql = insert_sql + ", ('"+str(datetime.now(timezone.utc))+"', "+\
                                "'"+str(datetime.now(timezone.utc))+"', '"+company["name"]+"', "+\
                                "'"+title+"', '"+url+"', "+\
                                "'BigKinds', '"+date+"', '"+company["code"]+"') "

                        insert_sql = insert_sql[1:]
                        try:
                            self.cur.execute("insert into stocks_news (created_at, updated_at, stock_name, "
                                             "title, link, source, date, code_id) values "+insert_sql)
                            self.db.commit()
                        except Exception as e:
                            self.db.rollback()
                            date_time = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time()))
                            with open(constant.error_file_path + "/big_kinds_news_error_list_" +
                                      time.strftime("%Y-%m-%d", time.localtime(time.time())) + ".txt", "a",
                                      encoding="UTF-8") as f:
                                f.write(date_time + "_" + company["code"][1:] + "_" + company["name"] + "\n")
                                f.write(traceback.format_exc())

                    # 스크래핑 수행한 종목 개수 카운트.
                    item_count = item_count + 1
                    # 목표 스크래핑 개수를 마치면 종료.
                    if item_count == self.scraping_count_goal:
                        return
                    if item_count % 30 == 0:
                        self.driver.quit()
                        self.initial_setting()

                    break

                except NoSuchWindowException as e:
                    # 에러 정보 저장.
                    date_time = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time()))
                    with open(constant.error_file_path+"/big_kinds_news_error_list_"+
                              time.strftime("%Y-%m-%d", time.localtime(time.time()))+".txt", "a", encoding="UTF-8") as f:
                        f.write(date_time + "_" + company["code"][1:] + "_" + company["name"] + "\n")
                        f.write(traceback.format_exc())

                    self.driver.quit()
                    time.sleep(random.uniform(4, 5))
                    self.initial_setting()

                    continue
                except Exception as e:
                    date_time = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time()))
                    with open(
                            constant.error_file_path+"/big_kinds_news_error_list_" + time.strftime("%Y-%m-%d", time.localtime(time.time())) + ".txt",
                            "a", encoding="UTF-8") as f:
                        f.write(date_time + "_" + company["code"][1:] + "_" + company["name"] + "\n")
                        f.write(traceback.format_exc())
                    time.sleep(random.uniform(3, 4))
                    continue

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
                    "//div[@class='deepsearch-app']/div[contains(@class,'drawer-container-layout')]/"
                    "div[contains(@class,'drawer-container')]/div[contains(@class,'drawer-container-inner')]/"
                    "div[contains(@class,'menu-item-group')][2]/div[contains(@class,'menu-item')][3]")
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



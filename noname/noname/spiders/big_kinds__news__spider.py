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
class BeKindsNewsSpider(scrapy.Spider):
    name = "big_kinds_news_spider";

    def __init__(self, start_date, end_date, scraping_count_goal=350):
        super(BeKindsNewsSpider, self).__init__()
        self.driver = None
        self.start_date = start_date
        self.start_date_arr = self.start_date.split("-")
        self.end_date = end_date
        self.end_date_arr = self.end_date.split("-")
        self.scraping_count_goal = int(scraping_count_goal)

        self.db = psycopg2.connect(host="112.220.72.179", dbname="openmetric", user="openmetric",
                              password=")!metricAdmin01", port=2345)
        self.cur = self.db.cursor()
        self.kospi_list = pd.read_sql("select * from stocks_basic_info where corp_code!=' '", self.db).sort_values(by="code")
        self.keyword_list = pd.read_sql("select * from social_keywords where corp_code!=' '", self.db).sort_values(by="code_id")

        # 시작시간, 중간 쉬는 시간, 종료시간 설정.
        today = time.localtime(time.time())
        self.start_time = datetime.datetime(today.tm_year, today.tm_mon, today.tm_mday,
                                            int(random.triangular(9, 10, 9)), int(random.randrange(0, 59, 1)))
        self.break_time = datetime.datetime(today.tm_year, today.tm_mon, today.tm_mday,
                                            int(random.triangular(12, 13, 13)), int(random.randrange(0, 59, 1)))
        self.end_time = datetime.datetime(today.tm_year, today.tm_mon, today.tm_mday,
                                          int(random.triangular(18, 20, 19)), int(random.randrange(0, 59, 1)))

    def start_requests(self):
        url_list = [
            "https://www.bigkinds.or.kr/"
        ]

        for url in url_list:
            yield scrapy.Request(url=url)

    def parse(self, response):
        try:
            self.initial_setting()
            # self.big_kinds_news_scraping()

            # 빅카인즈 엑셀 다운은 한번에 2만건 까지 밖에 안 받아지므로, 적절한 기간으로 나누어서 스파이더를 반복 실행하며 과거 뉴스 데이터를
            # 받아야 함. 이 함수는 적절한 기간으로 스파이더를 호출하는 기능을 담당.
            # 기준은 분기로 결정. (종목약명 중 가장 결과가 많을거라 예상되는 삼성, 네이버가 연 4,5만건임을 고려하여 결정)
            term_list = [
                ["01-01", "03-31"],
                ["04-01", "06-30"],
                ["07-01", "09-30"],
                ["10-01", "12-31"]
            ]
            year_term = [2000, 2020]
            for year in range(year_term[0], year_term[1]+1, 1):
                year = str(year)
                for i in range(4):
                    self.start_date = year+"-"+term_list[i][0]
                    self.end_date = year + "-"+term_list[i][1]
                    self.big_kinds_news_scraping()

        except Exception as e:
            date_time = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time()))
            with open(constant.error_file_path + "/big_kinds_news_error_list_" +
                      time.strftime("%Y-%m-%d", time.localtime(time.time())) + ".txt", "a", encoding="UTF-8") as f:
                f.write(date_time + "_전체 에러 \n")
                f.write(traceback.format_exc())

    # 분기별 재무정보 스크래핑 후 업데이트
    def big_kinds_news_scraping(self):
        # 디버깅용
        # self.kospi_list = self.kospi_list[:10]

        # 분기 데이터 받아야 하는 리스트 대상으로 한번 반복.
        item_count = 0 #반복시마다 증가하는 카운트.(크롬 out of memory오류 방지를 위해 체크)
        # 임시 항목 리스트에 대해 분기 데이터 추출.
        for index, company in self.kospi_list.iterrows():
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
                    self.wait(2, 1)
                    self.driver.find_element_by_xpath("//input[@id='total-search-key']").send_keys(company["name"])
                    self.wait(1, 1)
                    self.driver.find_element_by_xpath("//input[@id='total-search-key']").send_keys(Keys.ARROW_UP+Keys.ARROW_UP)
                    self.wait(1, 1)
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
                            self.wait(0, 1)
                        self.wait(2)
                        self.driver.find_element_by_xpath("//input[@id='search-begin-date']").send_keys(self.start_date)
                        self.wait(2)

                    temp_button = self.driver.find_element_by_xpath("//input[@id='search-end-date']")
                    if self.end_date != temp_button.get_attribute("value"):
                        for i in range(10):
                            self.driver.find_element_by_xpath("//input[@id='search-end-date']").send_keys(Keys.BACK_SPACE)
                            self.wait(0, 1)
                        self.wait(2)
                        self.driver.find_element_by_xpath("//input[@id='search-end-date']").send_keys(self.end_date)
                        self.wait(2)

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
                        self.wait(2)

                        and_include_keyword = ""
                        for keyword in and_include_keyword_list.split("\\"):
                            and_include_keyword = and_include_keyword+", "+keyword
                        self.driver.find_element_by_xpath("//input[@id='andKeyword1']").send_keys(and_include_keyword[2:])
                        self.wait(2)

                        exclude_keyword = ""
                        for keyword in exclude_keyword_list.split("\\"):
                            exclude_keyword = exclude_keyword+", "+keyword
                        self.driver.find_element_by_xpath("//input[@id='notKeyword1']").send_keys(exclude_keyword[2:])
                        self.wait(2)

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
                    self.wait(3)
                    # 다운 버튼 클릭.
                    self.click_element(
                        "//div[@id='analytics-data-download']/div[contains(@class,'btm-btn-wrp')]"
                        "/button[contains(@class,'news-download-btn')]", 2
                    )
                    self.driver.switch_to.alert.accept()
                    self.wait(2)

                    # 다운 완료 체크
                    for i in range(60):
                        if os.path.isfile(constant.download_path+"/bigkinds/NewsResult_"+self.start_date.replace("-","")+
                        "-"+self.end_date.replace("-","")+".xlsx"):
                            break;
                        else:
                            time.sleep(1)
                            if i == 59:
                                raise Exception("엑셀 파일 다운 대기 시간 초과.")
                    self.wait(5)

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
                    self.wait(4)
                    self.initial_setting()

                    continue
                except Exception as e:
                    date_time = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time()))
                    with open(
                            constant.error_file_path+"/big_kinds_news_error_list_" + time.strftime("%Y-%m-%d", time.localtime(time.time())) + ".txt",
                            "a", encoding="UTF-8") as f:
                        f.write(date_time + "_" + company["code"][1:] + "_" + company["name"] + "\n")
                        f.write(traceback.format_exc())
                    self.wait(3)
                    continue

    def click_element(self, xpath, wait_time):
        button = self.driver.find_element_by_xpath(xpath)
        try:
            button.click()
        except ElementNotInteractableException as e:
            self.driver.execute_script("arguments[0].click();", button)
        # except Exception as e:
        #     self.driver.execute_script("arguments[0].click();", button)
        self.wait(wait_time)

    def wait(self, wait_time, term=5):
        # 시작시간, 중간 쉬는 시간, 끝시간에 따른 대기.
        now = datetime.datetime.now()
        if (self.start_time.day == now.day) & (self.start_time > now):
            while self.start_time > now:
                time.sleep(10)
            else:
                self.start_time = self.start_time + datetime.timedelta(days=1)
                self.start_time = self.start_time.replace(hour=int(random.triangular(9,10,9)), minute=int(random.randrange(0,59,1)))

        elif (self.break_time.day == now.day) & (self.break_time < now) & (self.end_time > now):
            time.sleep(random.normalvariate(3000, 300))
            self.break_time = self.break_time + datetime.timedelta(days=1)
            self.break_time = self.break_time.replace(hour=int(random.triangular(12, 13, 13)),
                                                      minute=int(random.randrange(0, 59, 1)))

        elif (self.end_time.day == now.day) & (self.end_time < now):
            while datetime.datetime.now() > datetime.datetime(now.year, now.month, now.day+1, 6):
                time.sleep(10)
            self.end_time = self.end_time + datetime.timedelta(days=1)
            self.end_time = self.end_time.replace(hour=int(random.triangular(5,7,6)),
                                                      minute=int(random.randrange(0, 59, 1)))

        # 랜덤 몇 초 더 대기.
        random_value = random.randrange(1, 100, 1)
        if random_value % 20 == 0:
            time.sleep(random.triangular(wait_time, wait_time + term + 5, wait_time + term))
        time.sleep(random.triangular(wait_time, wait_time + term, wait_time))
        # 랜덤 3~5분 대기.
        random_value3 = random.randrange(1, 100, 1)
        if random_value3 % 100 == 0:
            time.sleep(random.uniform(180, 300))
        # 랜덤 10~20분 대기.
        random_value2 = random.randrange(1, 1000, 1)
        if random_value2 % 500 == 0:
            time.sleep(random.uniform(600, 1200))

    def initial_setting(self):
        for i in range(10):
            try:
                # driver 실행.
                chrome_driver = constant.chrome_driver_path
                chrome_options = Options()
                chrome_options.add_experimental_option("prefs", {
                    "download.default_directory": constant.download_path.replace("/", "\\") + "\\bigkinds",
                    "profile.content_settings.exceptions.automatic_downloads.*.setting": 1,
                    "plugins.always_open_pdf_externally": True
                })
                self.driver = webdriver.Chrome(chrome_driver, chrome_options=chrome_options)
                # self.driver.set_window_position(1300,0)
                self.driver.set_window_size(1400, 1000)
                self.driver.get('https://www.bigkinds.or.kr/')
                self.driver.implicitly_wait(5)
                self.wait(2)

                # 검색 페이지 세팅.
                # 로그인
                self.click_element("//header[@id='header']/div[contains(@class,'hd-top')]"
                       "//div[contains(@class,'login-area')]/button[contains(@class,'login-area-before')]",2)
                self.driver.find_element_by_xpath( "//input[@id='login-user-id']").send_keys("tony62@naver.com")
                self.wait(2)
                self.driver.find_element_by_xpath("//input[@id='login-user-password']").send_keys("**2TJRGHqzdw")
                self.wait(2)
                self.click_element("//button[@id='login-btn']", 2)

                # 뉴스 검색창 이동
                temp_button = self.driver.find_element_by_xpath(
                    "//header[@id='header']/div[contains(@class,'hd-gnb')]/div[contains(@class,'inner')]"
                    "/div[contains(@class,'gnb-wp')]/ul[contains(@class,'gnb-list')]"
                    "//a[contains(@class,'gnb-link') and contains(text(),'뉴스 분석')]"
                )
                ActionChains(self.driver).move_to_element(temp_button).perform()
                self.wait(1)
                self.click_element(
                    "//header[@id='header']/div[contains(@class,'hd-gnb')]/div[contains(@class,'inner')]"
                    "/div[contains(@class,'gnb-wp')]/ul[contains(@class,'gnb-list')]"
                    "//a[contains(@class,'gnb-link') and contains(text(),'뉴스 분석')]"
                    "/following-sibling::div[contains(@class,'gnb-sub')]//div[contains(@class,'gnb-depth2')][1]", 2
                )

            except Exception as e:
                date_time = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time()))
                with open(constant.error_file_path + "/big_kinds_news_error_list_" + time.strftime("%Y-%m-%d", time.localtime(
                            time.time())) + ".txt",
                        "a", encoding="UTF-8") as f:
                    f.write(date_time + "_초기 세팅 실패.\n")
                    f.write(traceback.format_exc())
                self.driver.quit()
                self.wait(4+i)
                continue
            else:
                break



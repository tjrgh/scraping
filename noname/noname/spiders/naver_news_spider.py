import random
import traceback
from shutil import which

import numpy as np
import pandas as pd
import psycopg2
import scrapy
import datetime
import time

from scrapy import Selector
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, NoSuchWindowException, WebDriverException, \
    ElementNotInteractableException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from . import constant_var as constant
from . import  common_util as cm

SELENIUM_DRIVER_NAME = 'chrome'
SELENIUM_DRIVER_EXECUTABLE_PATH = which('geckodriver')
SELENIUM_DRIVER_ARGUMENTS=['--headless']  # '--headless' if using chrome instead of firefox


class NaverNewsSpider(scrapy.Spider):
    """
    네이버 뉴스에 종목명을 검색하여 뉴스 정보 및 반응을 스크래핑하는 스파이더.
    """

    name = "naver_news_spider";

    def __init__(self, start_date, end_date, scraping_count_goal=20):
        super(NaverNewsSpider, self).__init__()
        self.driver = None
        self.start_date = start_date
        self.start_date_arr = self.start_date.split("-")
        self.end_date = end_date
        self.end_date_arr = self.end_date.split("-")
        self.scraping_count_goal = int(scraping_count_goal)
        self.search_count = 0

        self.db = psycopg2.connect(host="112.220.72.179", dbname="openmetric", user="openmetric",
                              password=")!metricAdmin01", port=2345)
        self.cur = self.db.cursor()
        self.stock_list = pd.read_sql("select * from stocks_basic_info where corp_code!=' '", self.db).sort_values(by="code")

        # 시작시간, 중간 쉬는 시간, 종료시간 설정.
        today = time.localtime(time.time())
        self.start_time= datetime.datetime(today.tm_year, today.tm_mon, today.tm_mday, int(random.triangular(9, 10, 9)), int(random.randrange(0, 59, 1)))
        self.break_time = datetime.datetime(today.tm_year, today.tm_mon, today.tm_mday, int(random.triangular(12, 13, 13)), int(random.randrange(0, 59, 1)))
        self.end_time = datetime.datetime(today.tm_year, today.tm_mon, today.tm_mday, int(random.triangular(18, 20, 19)), int(random.randrange(0, 59, 1)))

    def start_requests(self):
        url_list = [
            "https://www.naver.com/"
        ]

        for url in url_list:
            yield scrapy.Request(url=url)

    def parse(self, response):
        try:
            self.initial_setting()

            self.naver_news_scraping()

            self.driver.quit()

        except Exception as e:
            self.report_error(e, msg="스크래퍼 최상단 에러")

    # 분기별 재무정보 스크래핑 후 업데이트
    def naver_news_scraping(self):
        """
        데이터 스크래핑 함수.
        """

        for index, stock in self.stock_list.iterrows():
            try:
                self.driver.find_element_by_xpath("//input[@id='nx_query']").clear()
                cm.wait(2)
                self.driver.find_element_by_xpath("//input[@id='nx_query']").send_keys(stock["name"])
                cm.wait(2)
                self.click_element(
                    "//form[@id='nx_search_form']/fieldset/button[contains(@class,'bt_search')]", 2
                )
                self.search_count = self.search_count + 1

                # 결과 없는 경우.
                try:
                    WebDriverWait(self.driver, 5).until(EC.visibility_of_element_located((By.XPATH,
                            "//div[@id='main_pack']/div[contains(@class,'api_noresult_wrap')]")))
                    continue
                except Exception as e:
                    pass

                news_count = 0
                # 결과 목록 스크래핑.
                while True:
                    insert_sql = ""
                    # 현재 페이지에 대해 스크래핑.
                    current_page_list_cnt = len(self.driver.find_elements_by_xpath("//li[contains(@id,'sp_nws')]"))
                    for i in range(current_page_list_cnt):
                        try:
                            news_count = news_count + 1
                            # 기존 저장된 뉴스 데이터 가져오기
                            stored_news_list = pd.read_sql(
                                "select code_id, title, press, date from news_reactions where source='naver_news'", self.db
                            ).sort_values(by="date").astype("string")

                            temp_tag = self.driver.find_elements_by_xpath(
                                "//li[@id='sp_nws"+str(news_count)+"']//div[contains(@class,'news_info')]/div[contains(@class,'info_group')]/a"
                            )
                            if len(temp_tag)==2:
                                # 스포츠 뉴스인 경우 패스.
                                page_url = self.driver.find_element_by_xpath(
                                    "//li[@id='sp_nws" + str(news_count) + "']//div[contains(@class,'news_info')]"
                                   "/div[contains(@class,'info_group')]/a[2]"
                                ).get_attribute("href")
                                if "sports.news.naver.com" in page_url:
                                    continue

                                # 뉴스 상세 페이지 새탭에서 열림.
                                self.click_element(
                                    "//li[@id='sp_nws" + str(news_count) + "']//div[contains(@class,'news_info')]"
                                    "/div[contains(@class,'info_group')]/a[2]", 2
                                )

                                self.driver.switch_to.window(self.driver.window_handles[1])

                                title = ""
                                press = ""
                                date = ""
                                time = ""
                                origin_source = ""
                                emotion_count = 0
                                comment_count = 0
                                emotion_like_cnt = 0
                                emotion_warm_cnt = 0
                                emotion_sad_cnt = 0
                                emotion_angry_cnt = 0
                                emotion_want_cnt = 0

                                # 스포츠 뉴스인 경우.
                                # if "sports.news.naver.com" in self.driver.current_url:
                                #     press = self.driver.find_element_by_xpath(
                                #         "//span[@id='pressLogo']/a/img"
                                #     ).get_attribute("alt")
                                #     press = self.validate_sql(press)
                                #     title = self.driver.find_element_by_xpath(
                                #         "//div[contains(@class,'news_headline')]/h4[contains(@class,'title')]"
                                #     ).text
                                #     title = self.validate_sql(title)
                                #     temp_datetime = self.driver.find_element_by_xpath(
                                #         "//div[contains(@class,'news_headline')]/div[contains(@class,'info')]/span[1]"
                                #     ).text
                                #     temp_datetime = temp_datetime.split(" ")
                                #     date = temp_datetime[1].replace(".", "-").rstrip("-")
                                #     if temp_datetime[2] == "오전":
                                #         time = temp_datetime[2]
                                #     elif temp_datetime[2] == "오후":
                                #         time = str(int(temp_datetime[3].split(":")[0]) + 12) + ":" + \
                                #                temp_datetime[3].split(":")[1]
                                #     origin_source = self.driver.find_element_by_xpath(
                                #         "//div[contains(@class,'news_headline')]/div[contains(@class,'info')]/a"
                                #     ).get_attribute("href")
                                #     emotion_count = self.driver.find_element_by_xpath(
                                #         "//div[contains(@class,'news_work')]/div[contains(@class,'count')]"
                                #         "/div[contains(@class,'_reactionModule')]/a[contains(@class,'_face')]" \
                                #         "/span[contains(@class,'_count')]"
                                #     ).text
                                #     if emotion_count == "공감":
                                #         emotion_count = str(0)
                                #     comment_count = str(0)
                                #     emotion_like_cnt = self.driver.find_element_by_xpath(
                                #         "//div[contains(@class,'news_end_btn')]/div[contains(@class,'_reactionModule')]/ul"
                                #         "/li[contains(@class,'good')]//span[contains(@class,'u_likeit_list_count')]"
                                #     ).text
                                #     emotion_warm_cnt = self.driver.find_element_by_xpath(
                                #         "//div[contains(@class,'news_end_btn')]/div[contains(@class,'_reactionModule')]/ul"
                                #         "/li[contains(@class,'fan')]//span[contains(@class,'u_likeit_list_count')]"
                                #     ).text
                                #     emotion_sad_cnt = self.driver.find_element_by_xpath(
                                #         "//div[contains(@class,'news_end_btn')]/div[contains(@class,'_reactionModule')]/ul"
                                #         "/li[contains(@class,'sad')]//span[contains(@class,'u_likeit_list_count')]"
                                #     ).text
                                #     emotion_angry_cnt = self.driver.find_element_by_xpath(
                                #         "//div[contains(@class,'news_end_btn')]/div[contains(@class,'_reactionModule')]/ul"
                                #         "/li[contains(@class,'angry')]//span[contains(@class,'u_likeit_list_count')]"
                                #     ).text
                                #     emotion_want_cnt = self.driver.find_element_by_xpath(
                                #         "//div[contains(@class,'news_end_btn')]/div[contains(@class,'_reactionModule')]/ul"
                                #         "/li[contains(@class,'want')]//span[contains(@class,'u_likeit_list_count')]"
                                #     ).text
                                # else:
                                press = self.driver.find_element_by_xpath(
                                    "//div[@id='main_content']/div[contains(@class,'article_header')]"
                                    "/div[contains(@class,'press_logo')]/a/img"
                                ).get_attribute("title")
                                press = self.validate_sql(press)
                                title = self.driver.find_element_by_xpath("//h3[@id='articleTitle']").text
                                title = self.validate_sql(title)
                                temp_datetime = self.driver.find_element_by_xpath(
                                    "//div[@id='main_content']/div[contains(@class,'article_header')]"
                                    "/div[contains(@class,'article_info')]/div[contains(@class,'sponsor')]"
                                    "/span[contains(@class,'t11')][1]"
                                ).text
                                temp_datetime = temp_datetime.split(" ")
                                date = temp_datetime[0].replace(".", "-").rstrip("-")
                                if temp_datetime[1]=="오전":
                                    time = temp_datetime[2]
                                elif temp_datetime[1]=="오후":
                                    time = str(int(temp_datetime[2].split(":")[0])+12)+":"+temp_datetime[2].split(":")[1]
                                origin_source = self.driver.find_element_by_xpath(
                                    "//div[@id='main_content']/div[contains(@class,'article_header')]"
                                    "/div[contains(@class,'article_info')]/div[contains(@class,'sponsor')]"
                                    "/a[contains(@class,'btn_artialoriginal')]"
                                ).get_attribute("href")
                                emotion_count = self.driver.find_element_by_xpath(
                                    "//div[@id='main_content']/div[contains(@class,'article_header')]"
                                    "/div[contains(@class,'article_info')]/div[contains(@class,'sponsor')]"
                                    "/div[contains(@class,'article_btns')]/div[contains(@class,'article_btns_left')]"
                                    "/div[contains(@class,'_reactionModule')]/a[contains(@class,'_face')]" \
                                    "/span[contains(@class,'_count')]"
                                ).text.replace(",", "")
                                if emotion_count == "공감":
                                    emotion_count = str(0)
                                comment_count = self.driver.find_element_by_xpath(
                                    "//a[@id='articleTitleCommentCount']/span[contains(@class,'lo_txt')]"
                                ).text.replace(",", "")
                                if comment_count == "댓글":
                                    comment_count = str(0)
                                emotion_like_cnt = self.driver.find_element_by_xpath(
                                    "//div[@id='spiLayer']/div[contains(@class,'_reactionModule')]/ul"
                                    "/li[contains(@class,'good')]//span[contains(@class,'u_likeit_list_count')]"
                                ).text.replace(",", "")
                                emotion_warm_cnt = self.driver.find_element_by_xpath(
                                    "//div[@id='spiLayer']/div[contains(@class,'_reactionModule')]/ul"
                                    "/li[contains(@class,'warm')]//span[contains(@class,'u_likeit_list_count')]"
                                ).text.replace(",", "")
                                emotion_sad_cnt = self.driver.find_element_by_xpath(
                                    "//div[@id='spiLayer']/div[contains(@class,'_reactionModule')]/ul"
                                    "/li[contains(@class,'sad')]//span[contains(@class,'u_likeit_list_count')]"
                                ).text.replace(",", "")
                                emotion_angry_cnt = self.driver.find_element_by_xpath(
                                    "//div[@id='spiLayer']/div[contains(@class,'_reactionModule')]/ul"
                                    "/li[contains(@class,'angry')]//span[contains(@class,'u_likeit_list_count')]"
                                ).text.replace(",", "")
                                emotion_want_cnt = self.driver.find_element_by_xpath(
                                    "//div[@id='spiLayer']/div[contains(@class,'_reactionModule')]/ul"
                                    "/li[contains(@class,'want')]//span[contains(@class,'u_likeit_list_count')]"
                                ).text.replace(",", "")

                                cm.wait(1, 1)
                                self.driver.close()
                                cm.wait(1, 1)
                                self.driver.switch_to.window(self.driver.window_handles[0])
                                cm.wait(1, 1)

                                # 뉴스 중목 체크
                                if(
                                        (title == stored_news_list["title"])
                                    & (press == stored_news_list["press"])
                                    & (date == stored_news_list["date"])
                                ).any():
                                    continue
                                else:
                                    # insert sql 생성.
                                    insert_sql = insert_sql + ", ("+\
                                    "'"+str(datetime.datetime.now(datetime.timezone.utc))+"', "+\
                                    "'"+str(datetime.datetime.now(datetime.timezone.utc))+"', "+\
                                    "'"+stock["name"]+"', '"+title+"', '"+origin_source+"', '"+press+"', "+\
                                    "'naver_news', '"+date+"', "+emotion_count+", "+comment_count+", "+\
                                    emotion_like_cnt+", "+emotion_warm_cnt+", "+emotion_sad_cnt+", "+\
                                    emotion_angry_cnt+", "+emotion_want_cnt+", '"+stock["code"]+"')"

                            else:
                                continue

                        except Exception as e:
                            self.report_error(e, stock["code"], stock["name"], "결과 리스트 한 페이지 스크래핑 중 에러")
                            while len(self.driver.window_handles) > 1:
                                self.driver.switch_to.window(self.driver.window_handles[1])
                                self.driver.close()
                            self.driver.switch_to.window(self.driver.window_handles[0])
                            continue

                    # db insert.
                    if insert_sql != "":
                        try:
                            self.cur.execute(
                                "insert into news_reactions ("
                                "   created_at, updated_at, stock_name, title, link, press, source, date, "
                                "   emotion_count, comment_count, emotion_like_cnt, emotion_warm_cnt, "
                                "   emotion_sad_cnt, emotion_angry_cnt, emotion_want_cnt, code_id"
                                ") values " + insert_sql[1:]
                            )
                            self.db.commit()
                        except Exception as e:
                            self.report_error(e, stock["code"], stock["name"], "db insert 에러")
                            self.db.rollback()


                    # 페이지 이동. 마지막이면 반복 정지.
                    paging_allow = self.driver.find_element_by_xpath(
                        "//div[@id='main_pack']/div[contains(@class,'api_sc_page_wrap')]//a[contains(@class,'btn_next')]"
                    )
                    if paging_allow.get_attribute("aria-disabled") == "true":
                        break
                    else:
                        self.click_element(
                            "//div[@id='main_pack']/div[contains(@class,'api_sc_page_wrap')]"
                            "//a[contains(@class,'btn_next')]", 2
                        )

                # 크롬 메모리 out 방지 및 검색 횟수 제한
                if (self.search_count % 30 == 0):
                    self.driver.quit()
                    cm.wait(4)
                    self.initial_setting()
                # 검색 회수 제한.
                self.search_count = cm.wait(60, search_count=self.search_count, search_count_max=self.scraping_count_goal)["search_count"]

            except NoSuchWindowException as e:
                self.report_error(e, stock["code"], stock["name"])
                self.driver.quit()
                cm.wait(4)
                self.initial_setting()
                continue

            except Exception as e:
                self.report_error(e, stock["code"], stock["name"], "종목 반복 중 에러")
                continue

    def validate_sql(self, sql):
        sql = sql.replace("'", "''")
        sql = sql.replace('"', "\"")

        return sql

    def report_error(self, e=None, code="", stock_name="", msg=""):
        date_time = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time()))
        with open(constant.error_file_path + "/naver_news_error_list_" +
                  time.strftime("%Y-%m-%d", time.localtime(time.time())) + ".txt", "a", encoding="UTF-8") as f:
            f.write(date_time + "_"+code+"_"+stock_name+"_"+msg+"\n")
            f.write(traceback.format_exc())

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
                print(str(datetime.datetime.now()) + "search count limit break term start.")
                self.report_error(msg="search count limit break term start.")
                while (datetime.datetime.now().time() < datetime.time(6, 0)) | \
                        (datetime.datetime.now().time() > datetime.time(7, 0)):
                    time.sleep(10)
                else:
                    print(str(datetime.datetime.now()) + "search count limit break term end.")
                    self.report_error(msg="search count limit break term end.")
                    self.start_time = datetime.time(int(random.triangular(9, 10, 9)),
                                                    int(random.randrange(0, 59, 1)))
                    self.break_time = datetime.time(int(random.triangular(12, 13, 13)),
                                                    int(random.randrange(0, 59, 1)))
                    self.end_time = datetime.time(int(random.triangular(17, 19, 18)),
                                                  int(random.randrange(0, 59, 1)))
                    self.search_count = 0

        # 시작시간, 중간 쉬는 시간, 끝시간에 따른 대기.
        if (self.start_time >= now) | (self.end_time <= now):
            if self.start_time >= now:
                print(str(datetime.datetime.now()) + "start break term start.")
            elif self.end_time <= now:
                print(str(datetime.datetime.now()) + "end break term start.")
            self.report_error(msg="start break term start.")
            while (self.start_time >= datetime.datetime.now().time()) | \
                    (self.end_time <= datetime.datetime.now().time()):
                time.sleep(10)
            else:
                print(str(datetime.datetime.now()) + "start/end break term end.")
                self.report_error(msg="start break term end.")
                self.search_count = 0
                self.start_time = datetime.time(int(random.triangular(9, 10, 9)),
                                                int(random.randrange(0, 59, 1)))
                self.end_time = datetime.time(int(random.triangular(17, 19, 19)),
                                              int(random.randrange(0, 59, 1)))
                self.break_time = datetime.time(int(random.triangular(12, 13, 13)),
                                                int(random.randrange(0, 30, 1)))
                print((str(datetime.datetime.now()) + "break_time : " + str(self.break_time)))
                print((str(datetime.datetime.now()) + "start_time : " + str(self.start_time)))
                print((str(datetime.datetime.now()) + "end_time : " + str(self.end_time)))

        elif (self.break_time < now) & \
                ((datetime.datetime.combine(datetime.date.today(), self.break_time)
                  + datetime.timedelta(minutes=30)).time() > now):
            print(str(datetime.datetime.now()) + "middle break term start.")
            self.report_error(msg="middle break term start.")
            time.sleep(random.normalvariate(3000, 400))
            print(str(datetime.datetime.now()) + "middle break term end.")
            self.report_error(msg="middle break term end.")

        # 랜덤 몇 초 더 대기.
        random_value = random.randrange(1, 100, 1)
        if random_value % 20 == 0:
            print(str(datetime.datetime.now()) + "more sleep...")
            time.sleep(random.triangular(wait_time, wait_time + term + 10, wait_time + term + 5))
        # print(str(datetime.datetime.now()) + "...")
        time.sleep(random.triangular(wait_time, wait_time + term, wait_time))
        # 랜덤 3~5분 대기.
        random_value3 = random.randrange(1, 100, 1)
        if random_value3 % 100 == 0:
            print(str(datetime.datetime.now()) + "3~5minute sleep")
            self.report_error(msg="3~5minute sleep")
            time.sleep(random.uniform(180, 300))
        # 랜덤 10~20분 대기.
        random_value2 = random.randrange(1, 1000, 1)
        if random_value2 % 500 == 0:
            print(str(datetime.datetime.now()) + "10~20minute sleep")
            self.report_error(msg="10~20minute sleep")
            time.sleep(random.uniform(600, 1200))

    def initial_setting(self):
        for i in range(10):
            try:
                # driver 실행.
                chrome_driver = constant.chrome_driver_path
                chrome_options = Options()
                # chrome_options.add_experimental_option("prefs", {
                    # "download.default_directory": constant.download_path.replace("/", "\\") + "\\sometrend",
                    # "profile.content_settings.exceptions.automatic_downloads.*.setting": 1,
                    # "plugins.always_open_pdf_externally": True
                # })
                self.driver = webdriver.Chrome(chrome_driver, chrome_options=chrome_options)
                # self.driver.set_window_position(1300,0)
                self.driver.get('https://www.naver.com/')
                self.driver.implicitly_wait(5)
                cm.wait(2)

                # 네이버 뉴스 검색 결과 페이지 이동
                self.driver.find_element_by_xpath("//input[@id='query']").send_keys("코스피")
                cm.wait(2)
                self.click_element("//button[@id='search_btn']", 2)

                self.click_element("//div[@id='lnb']/div[contains(@class,'lnb_group')]//ul[contains(@class,'base')]"
                                   "//a[contains(@class,'tab') and contains(text(),'뉴스')]", 2)

                # 검색 조건 설정
                self.click_element("//div[@id='snb']/div[contains(@class,'api_group_option_filter')]"
                                   "//div[contains(@class,'option_filter')]/a", 2)
                self.click_element(  # 기간 직접입력 클릭.
                    "//div[@id='snb']/div[contains(@class,'api_group_option_sort')]/ul[contains(@class,'lst_option')]"
                    "//strong[contains(@class,'tit') and contains(text(),'기간')]"
                    "/following::div[contains(@class,'option')]/a[contains(text(),'직접입력')]", 2
                )
                set_term_xpath = "//div[@id='snb']/div[contains(@class,'api_group_option_sort')]/ul[contains(@class,'lst_option')]" \
                                 "//strong[contains(@class,'tit') and contains(text(),'기간')]" \
                                 "/following::div[contains(@class,'_calendar_select_layer')]"
                self.click_element(set_term_xpath + "//a[contains(@class,'_start_trigger')]", 2)  # 시작일 선택 클릭.
                self.click_element(set_term_xpath + "/div[contains(@class,'select_wrap')]"  # 연 선택.
                                                    "/div[1]//ul[contains(@class,'lst_item')]/li[contains(@data-value,'" +
                                   self.start_date_arr[0] + "')]", 2)
                self.click_element(set_term_xpath + "/div[contains(@class,'select_wrap')]"  # 월 선택
                                                    "/div[2]//ul[contains(@class,'lst_item')]/li[contains(@data-value,'" + str(
                    int(self.start_date_arr[1])) + "')]", 2)
                self.click_element(set_term_xpath + "/div[contains(@class,'select_wrap')]"  # 일 선택.
                                                    "/div[3]//ul[contains(@class,'lst_item')]/li[contains(@data-value,'" + str(
                    int(self.start_date_arr[2])) + "')]", 2)

                self.click_element(set_term_xpath + "//a[contains(@class,'_end_trigger')]", 2)  # 종료일 선택 클릭.
                self.click_element(set_term_xpath + "/div[contains(@class,'select_wrap')]"  # 연 선택.
                                                    "/div[1]//ul[contains(@class,'lst_item')]/li[contains(@data-value,'" +
                                   self.end_date_arr[0] + "')]", 2)
                self.click_element(set_term_xpath + "/div[contains(@class,'select_wrap')]"  # 월 선택
                                                    "/div[2]//ul[contains(@class,'lst_item')]/li[contains(@data-value,'" + str(
                    int(self.end_date_arr[1])) + "')]", 2)
                self.click_element(set_term_xpath + "/div[contains(@class,'select_wrap')]"  # 일 선택.
                                                    "/div[3]//ul[contains(@class,'lst_item')]/li[contains(@data-value,'" + str(
                    int(self.end_date_arr[2])) + "')]", 2)

                self.click_element(  # 적용.
                    set_term_xpath + "/div[contains(@class,'btn_area')]/button", 2
                )
                self.click_element(  # 최신순 선택.
                    "//div[@id='snb']/div[contains(@class,'api_group_option_sort')]/ul[contains(@class,'lst_option')]"
                    "//strong[contains(@class,'tit') and contains(text(),'정렬')]"
                    "/following::div[contains(@class,'option')]/a[contains(text(),'최신순')]", 2
                )

            except Exception as e:
                date_time = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time()))
                with open(constant.error_file_path + "/naver_news_error_list_" + time.strftime("%Y-%m-%d", time.localtime(
                            time.time())) + ".txt",
                        "a", encoding="UTF-8") as f:
                    f.write(date_time + "_초기 세팅 실패.\n")
                    f.write(traceback.format_exc())
                self.driver.quit()
                cm.wait(5+i)
                continue
            else:
                break



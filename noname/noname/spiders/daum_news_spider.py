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
    ElementNotInteractableException, ElementClickInterceptedException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from . import constant_var as constant
from . import common_util as cm

SELENIUM_DRIVER_NAME = 'chrome'
SELENIUM_DRIVER_EXECUTABLE_PATH = which('geckodriver')
SELENIUM_DRIVER_ARGUMENTS=['--headless']  # '--headless' if using chrome instead of firefox


class DaumNewsSpider(scrapy.Spider):
    """
    네이버 뉴스에 종목명을 검색하여 뉴스 정보 및 반응을 스크래핑하는 스파이더.
    """

    name = "daum_news_spider";

    def __init__(self, start_date, end_date, scraping_count_goal=20):
        super(DaumNewsSpider, self).__init__()
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
            "https://www.daum.net/"
        ]

        for url in url_list:
            yield scrapy.Request(url=url)

    def parse(self, response):
        try:
            self.initial_setting()
            self.daum_news_scraping()

            self.driver.quit()

        except Exception as e:
            self.report_error(e, msg="스크래퍼 최상단 에러")

    # 분기별 재무정보 스크래핑 후 업데이트
    def daum_news_scraping(self):
        """
        데이터 스크래핑 함수.
        """

        for index, stock in self.stock_list.iterrows():
            try:
                self.driver.find_element_by_xpath("//input[@id='q']").clear()
                cm.wait(2)
                self.driver.find_element_by_xpath("//input[@id='q']").send_keys(stock["name"])
                # self.driver.find_element_by_xpath("//input[@id='q']").send_keys("두산베어스")
                cm.wait(2)
                self.click_element("//button[@id='daumBtnSearch']", 2 )
                self.search_count = self.search_count + 1

                # 결과 없는 경우.
                if self.check_element("//div[@id='noResult']") == True:
                    continue

                news_count = 0
                # 결과 목록 스크래핑.
                while True:
                    insert_sql = ""

                    # 기존 저장된 뉴스 데이터 가져오기
                    stored_news_list = pd.read_sql(
                        "select code_id, title, press, date from news_reactions where source='daum_news'", self.db
                    ).sort_values(by="date").astype("string")

                    # 현재 페이지에 대해 스크래핑.
                    current_page_list_xpath = "//div[@id='newsColl']/div[contains(@class,'cont_divider')]" \
                                              "/ul[contains(@class,'list_news')]/li"
                    current_page_list_cnt = len(self.driver.find_elements_by_xpath(current_page_list_xpath))
                    for i in range(1, current_page_list_cnt+1, 1):
                        try:
                            news_count = news_count + 1

                            # 다음뉴스 페이지 존재 확인.
                            if self.check_element(
                                    current_page_list_xpath + "[" + str(i) + "]/div[contains(@class,'wrap_cont')]"
                                    "/span[contains(@class,'cont_info')]/a[contains(text(),'다음뉴스')]"
                            ) == False:
                                continue

                            # 원문 링크 url
                            origin_source = self.driver.find_element_by_xpath(
                                current_page_list_xpath+"["+str(i)+"]/div[contains(@class,'wrap_cont')]"
                                "/a[contains(@class,'tit_main')]"
                            ).get_attribute("href")

                            # 뉴스 상세 페이지 새탭에서 열림.
                            self.click_element(
                                current_page_list_xpath + "[" + str(i) + "]/div[contains(@class,'wrap_cont')]"
                                "/span[contains(@class,'cont_info')]/a[contains(text(),'다음뉴스')]", 2
                            )

                            self.driver.switch_to.window(self.driver.window_handles[1])

                            # 스포츠일 경우 패스
                            if ("sports.v.daum.net" in self.driver.current_url):
                                continue

                            title = ""
                            press = ""
                            date = ""
                            time = ""
                            emotion_count = '0'
                            comment_count = '0'
                            emotion_like_cnt = '0'
                            emotion_warm_cnt = '0'
                            emotion_sad_cnt = '0'
                            emotion_angry_cnt = '0'
                            emotion_recommend_cnt = '0'
 
                            #언론사
                            press = self.driver.find_element_by_xpath(
                                "//div[@id='cSub']//em[contains(@class,'info_cp')]//img"
                            ).get_attribute("alt")
                            press = self.validate_sql(press)
                            # 제목
                            title = self.driver.find_element_by_xpath(
                                "//div[@id='cSub']//h3[contains(@class,'tit_view')]"
                            ).text
                            title = self.validate_sql(title)
                            # 날짜
                            temp_datetime = self.driver.find_element_by_xpath(
                                "//div[@id='cSub']//span[contains(@class,'info_view')]"
                                "/span[contains(@class,'txt_info') and contains(text(),'입력')]/span[contains(@class,'num_date')]"
                            ).text
                            temp_datetime = temp_datetime.replace(" ", "").split(".")
                            date = temp_datetime[0]+"-"+temp_datetime[1]+"-"+temp_datetime[2]
                            # 댓글 개수
                            if (("sports.v.daum.net" not in self.driver.current_url)
                                and ("entertain.v.daum.net" not in self.driver.current_url)
                            ):
                                comment_count = self.driver.find_element_by_xpath(
                                    "//button[@id='alexCounter']/span[contains(@class,'alex-count-area')]"
                                ).text
                            #  감정 반응
                            emotion_list_xpath = "//div[@id='mArticle']/div[contains(@class,'foot_view')]" \
                                                 "/div[contains(@class,'emotion_wrap')]/div[contains(@class,'emotion_list')]" \
                                                 "//div[contains(@class,'list-wrapper')]/div"
                            emotion_like_cnt = self.driver.find_element_by_xpath(
                                emotion_list_xpath+"[2]/span[contains(@class,'count')]"
                            ).text
                            emotion_warm_cnt = self.driver.find_element_by_xpath(
                                emotion_list_xpath+"[3]/span[contains(@class,'count')]"
                            ).text
                            emotion_sad_cnt = self.driver.find_element_by_xpath(
                                emotion_list_xpath+"[5]/span[contains(@class,'count')]"
                            ).text
                            emotion_angry_cnt = self.driver.find_element_by_xpath(
                                emotion_list_xpath+"[4]/span[contains(@class,'count')]"
                            ).text
                            emotion_recommend_cnt = self.driver.find_element_by_xpath(
                                emotion_list_xpath+"[1]/span[contains(@class,'count')]"
                            ).text

                            emotion_count = str(int(emotion_like_cnt)+int(emotion_warm_cnt)+\
                                int(emotion_recommend_cnt)+int(emotion_angry_cnt)+\
                                int(emotion_sad_cnt))

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
                                "'daum_news', '"+date+"', "+emotion_count+", "+comment_count+", "+\
                                emotion_like_cnt+", "+emotion_warm_cnt+", "+emotion_sad_cnt+", "+\
                                emotion_angry_cnt+", "+emotion_recommend_cnt+", '"+stock["code"]+"')"

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
                                "   emotion_sad_cnt, emotion_angry_cnt, emotion_recommend_cnt, code_id"
                                ") values " + insert_sql[1:]
                            )
                            self.db.commit()
                        except Exception as e:
                            self.report_error(e, stock["code"], stock["name"], "db insert 에러")
                            self.db.rollback()


                    # 페이지 이동. 마지막이면 반복 정지.
                    paging_allow = self.driver.find_element_by_xpath(
                        "//div[@id='newsColl']/div[contains(@class,'paging_comm')]"
                        "//span[contains(@data-paging-area,'next')]/*[@data-paging-active]"
                    )
                    if paging_allow.get_attribute("data-paging-active") == "false":
                        break
                    else:
                        self.click_element(
                            "//div[@id='newsColl']/div[contains(@class,'paging_comm')]"
                            "//span[contains(@data-paging-area,'next')]/*[@data-paging-active]", 2
                        )

                # 크롬 메모리 out 방지 및 검색 횟수 제한
                if (self.search_count % 30 == 0):
                    self.driver.quit()
                    cm.wait(4)
                    self.initial_setting()
                # 검색 회수 제한.
                self.search_count = cm.wait(30, search_count=self.search_count, search_count_max=self.scraping_count_goal)["search_count"]

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

    def check_element(self, xpath, condition=EC.visibility_of_element_located, wait_time=5):
        try:
            WebDriverWait(self.driver, wait_time).until(condition((By.XPATH, xpath)))
            cm.wait(3)
            return True
        except Exception as e:
            return False

    def report_error(self, e=None, code="", stock_name="", msg=""):
        date_time = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time()))
        with open(constant.error_file_path + "/daum_news_error_list_" +
                  time.strftime("%Y-%m-%d", time.localtime(time.time())) + ".txt", "a", encoding="UTF-8") as f:
            f.write(date_time + "_"+code+"_"+stock_name+"_"+msg+"\n")
            f.write(traceback.format_exc())

    def click_element(self, xpath, wait_time, term=None):
        button = self.driver.find_element_by_xpath(xpath)
        try:
            button.click()
        except ElementNotInteractableException as e:
            self.driver.execute_script("arguments[0].click();", button)
        except ElementClickInterceptedException as e:
            self.driver.execute_script("arguments[0].click();", button)
        # except Exception as e:
        #     self.driver.execute_script("arguments[0].click();", button)
        if term == None:
            cm.wait(wait_time)
        else:
            cm.wait(wait_time, term)

    def wait(self, wait_time,  term=5, search_count=None):
        now = datetime.datetime.now()
        # 검색 쿼리 횟수 제한
        if (search_count!=None) :
            if(self.search_count >= self.scraping_count_goal):
                self.report_error(msg="search count limit break term start.")
                while datetime.datetime.now() > datetime.datetime(now.year, now.month, now.day+1, 6):
                    time.sleep(10)
                else:
                    self.report_error(msg="search count limit break term end.")
                    self.start_time = self.start_time + datetime.timedelta(days=1)
                    self.start_time = self.start_time.replace(hour=int(random.triangular(9, 10, 9)),
                                                              minute=int(random.randrange(0, 59, 1)))
                    self.break_time = self.break_time + datetime.timedelta(days=1)
                    self.break_time = self.break_time.replace(hour=int(random.triangular(12, 13, 13)),
                                                              minute=int(random.randrange(0, 59, 1)))
                    self.end_time = self.end_time + datetime.timedelta(days=1)
                    self.end_time = self.end_time.replace(hour=int(random.triangular(5, 7, 6)),
                                                          minute=int(random.randrange(0, 59, 1)))
                    self.search_count = 0

        # 시작시간, 중간 쉬는 시간, 끝시간에 따른 대기.
        # if (self.start_time.day == now.day) & (self.start_time > now):
        #     self.report_error(msg="start break term start.")
        #     while self.start_time > now:
        #         time.sleep(10)
        #     else:
        #         self.report_error(msg="start break term end.")
        #         self.start_time = self.start_time + datetime.timedelta(days=1)
        #         self.start_time = self.start_time.replace(hour=int(random.triangular(9,10,9)), minute=int(random.randrange(0,59,1)))
        #
        # elif (self.break_time.day == now.day) & (self.break_time < now) & (self.end_time > now):
        #     self.report_error(msg="middle break term start.")
        #     time.sleep(random.normalvariate(3000, 300))
        #     self.report_error(msg="middle break term end.")
        #     self.break_time = self.break_time + datetime.timedelta(days=1)
        #     self.break_time = self.break_time.replace(hour=int(random.triangular(12, 13, 13)),
        #                                               minute=int(random.randrange(0, 59, 1)))
        #
        # elif (self.end_time.day == now.day) & (self.end_time < now):
        #     self.report_error(msg="end break term start.")
        #     while datetime.datetime.now() > datetime.datetime(now.year, now.month, now.day+1, 6):
        #         time.sleep(10)
        #     self.report_error(msg="end break term end.")
        #     self.end_time = self.end_time + datetime.timedelta(days=1)
        #     self.end_time = self.end_time.replace(hour=int(random.triangular(5,7,6)),
        #                                               minute=int(random.randrange(0, 59, 1)))

        # 랜덤 몇 초 더 대기.
        random_value = random.randrange(1, 100, 1)
        # if random_value % 20 == 0:
        #     time.sleep(random.triangular(wait_time, wait_time + term + 10, wait_time + term+5))
        time.sleep(random.triangular(wait_time, wait_time + term, wait_time))
        # 랜덤 3~5분 대기.
        random_value3 = random.randrange(1, 100, 1)
        if random_value3 % 100 == 0:
            self.report_error(msg="3~5minute sleep")
            # time.sleep(random.uniform(180, 300))
        # 랜덤 10~20분 대기.
        random_value2 = random.randrange(1, 1000, 1)
        if random_value2 % 500 == 0:
            self.report_error(msg="10~20minute sleep")
            # time.sleep(random.uniform(600, 1200))

    def initial_setting(self):
        for i in range(10):
            try:
                # driver 실행.
                chrome_driver = constant.chrome_driver_path
                chrome_options = Options() 
                chrome_options.add_argument("--headless")
                chrome_options.add_argument("--no-sandbox")
                # chrome_options.add_experimental_option("prefs", {
                    # "download.default_directory": constant.download_path.replace("/", "\\") + "\\sometrend",
                    # "profile.content_settings.exceptions.automatic_downloads.*.setting": 1,
                    # "plugins.always_open_pdf_externally": True
                # })
                self.driver = webdriver.Chrome(chrome_driver, chrome_options=chrome_options)
                # self.driver.set_window_position(1300,0)
                self.driver.get('https://www.daum.net/')
                self.driver.implicitly_wait(5)
                cm.wait(2)

                # 다음 뉴스 검색 결과 페이지 이동
                self.driver.find_element_by_xpath("//input[@id='q']").send_keys("코스피")
                cm.wait(2)
                self.click_element("//input[@id='q']/following-sibling::button[contains(@class,'btn_search')]", 2)

                self.click_element("//div[@id='daumGnb']//ul[contains(@class,'gnb_search')]"
                                   "//span[contains(@class,'txt_tab') and contains(text(),'뉴스')]", 2)

                # 검색 조건 설정
                search_option_xpath = "//div[@id='newsColl']/div[contains(@class,'cont_divider')]"\
                                   "/div[contains(@class,'compo-tabopt')]/div[contains(@class,'box_opt')]"
                self.click_element(search_option_xpath +
                                   "/div[contains(@data-option-type,'period')]/a", 2)
                # 시작일 입력
                self.click_element(
                    search_option_xpath + "/div[contains(@data-option-type,'period')]"
                    "/div[contains(@class,'layer_opt')]/div[contains(@class,'box_date')]"
                    "/div[contains(@class,'item_date')][1]", 2
                )
                self.click_element(
                    "//div[@id='ui-datepicker-div']/div[contains(@class,'ui-datepicker-header')]"
                    "/div[contains(@class,'ui-datepicker-title')]/select[contains(@class,'year')]", 2
                )
                self.click_element(
                    "//div[@id='ui-datepicker-div']/div[contains(@class,'ui-datepicker-header')]"
                    "/div[contains(@class,'ui-datepicker-title')]/select[contains(@class,'year')]"
                    "/option[contains(@value,'"+self.start_date_arr[0]+"')]", 2
                )
                self.click_element(
                    "//div[@id='ui-datepicker-div']/div[contains(@class,'ui-datepicker-header')]"
                    "/div[contains(@class,'ui-datepicker-title')]/select[contains(@class,'month')]", 2
                )
                self.click_element(
                    "//div[@id='ui-datepicker-div']/div[contains(@class,'ui-datepicker-header')]"
                    "/div[contains(@class,'ui-datepicker-title')]/select[contains(@class,'month')]"
                    "/option[contains(text(),'" + str(int(self.start_date_arr[1])) + "')]", 2
                )
                self.click_element(
                    "//div[@id='ui-datepicker-div']/table[contains(@class,'ui-datepicker-calendar')]"
                    "//td[not(contains(@class,'ui-datepicker-other-month'))]"
                    "/a[contains(text(),'"+str(int(self.start_date_arr[2]))+"')]", 2
                )
                # 종료일 입력
                self.click_element(
                    search_option_xpath + "/div[contains(@data-option-type,'period')]"
                                          "/div[contains(@class,'layer_opt')]/div[contains(@class,'box_date')]"
                                          "/div[contains(@class,'item_date')][2]", 2
                )
                self.click_element(
                    "//div[@id='ui-datepicker-div']/div[contains(@class,'ui-datepicker-header')]"
                    "/div[contains(@class,'ui-datepicker-title')]/select[contains(@class,'year')]", 2
                )
                self.click_element(
                    "//div[@id='ui-datepicker-div']/div[contains(@class,'ui-datepicker-header')]"
                    "/div[contains(@class,'ui-datepicker-title')]/select[contains(@class,'year')]"
                    "/option[contains(@value,'" + self.end_date_arr[0] + "')]", 2
                )
                self.click_element(
                    "//div[@id='ui-datepicker-div']/div[contains(@class,'ui-datepicker-header')]"
                    "/div[contains(@class,'ui-datepicker-title')]/select[contains(@class,'month')]", 2
                )
                self.click_element(
                    "//div[@id='ui-datepicker-div']/div[contains(@class,'ui-datepicker-header')]"
                    "/div[contains(@class,'ui-datepicker-title')]/select[contains(@class,'month')]"
                    "/option[contains(text(),'" + str(int(self.end_date_arr[1])) + "')]", 2
                )
                self.click_element(
                    "//div[@id='ui-datepicker-div']/table[contains(@class,'ui-datepicker-calendar')]"
                    "//td[not(contains(@class,'ui-datepicker-other-month'))]"
                    "/a[contains(text(),'" + str(int(self.end_date_arr[2])) + "')]", 2
                )
                # 적용
                self.click_element(
                    search_option_xpath + "/div[contains(@data-option-type,'period')]"
                    "/div[contains(@class,'layer_opt')]/div[contains(@class,'box_date')]"
                    "/button[contains(@class,'btn_confirm')]", 5
                )
                # 옵션 유지 설정
                self.click_element(
                    search_option_xpath+"/label[contains(@class,'opt_keep')]/span", 2
                )

            except Exception as e:
                self.report_error(e, msg="초기 세팅 실패")
                self.driver.quit()
                cm.wait(5+i)
                continue
            else:
                break



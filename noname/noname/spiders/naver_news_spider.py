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

SELENIUM_DRIVER_NAME = 'chrome'
SELENIUM_DRIVER_EXECUTABLE_PATH = which('geckodriver')
SELENIUM_DRIVER_ARGUMENTS=['--headless']  # '--headless' if using chrome instead of firefox


class NaverNewsSpider(scrapy.Spider):
    """
    네이버 뉴스에 종목명을 검색하여 뉴스 정보 및 반응을 스크래핑하는 스파이더.
    """

    name = "naver_news_spider";

    def __init__(self, start_date, end_date, scraping_count_goal=200):
        super(NaverNewsSpider, self).__init__()
        self.driver = None
        self.start_date = start_date
        self.start_date_arr = self.start_date.split("-")
        self.end_date = end_date
        self.end_date_arr = self.end_date.split("-")
        self.scraping_count_goal = int(scraping_count_goal)

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

        except Exception as e:
            date_time = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time()))
            with open(constant.error_file_path + "/fnguide_report_summary_error_list_" +
                      time.strftime("%Y-%m-%d", time.localtime(time.time())) + ".txt", "a", encoding="UTF-8") as f:
                f.write(date_time + "_전체 에러 \n")
                f.write(traceback.format_exc())

    # 분기별 재무정보 스크래핑 후 업데이트
    def naver_news_scraping(self):
        """
        데이터 스크래핑 함수.
        """
        search_count = 0

        for index, stock in self.stock_list.iterrows():

            self.driver.find_element_by_xpath("//input[@id='nx_query']").clear()
            self.driver.find_element_by_xpath("//input[@id='nx_query']").send_keys(stock["name"])
            self.click_element(
                "//form[@id='nx_search_form']/fieldset/button[contains(@class,'bt_search')]", 2
            )
            # 검색 조건 설정
            self.click_element("//div[@id='snb']/div[contains(@class,'api_group_option_filter')]"
                               "//div[contains(@class,'option_filter')]/a", 2)
            self.click_element(# 기간 직접입력 클릭.
                "//div[@id='snb']/div[contains(@class,'api_group_option_sort')]/ul[contains(@class,'lst_option')]"
                "//strong[contains(@class,'tit') and contains(text(),'기간')]"
                "/following::div[contains(@class,'option')]/a[contains(text(),'직접입력')]", 2
            )
            set_term_xpath = "//div[@id='snb']/div[contains(@class,'api_group_option_sort')]/ul[contains(@class,'lst_option')]"\
                "//strong[contains(@class,'tit') and contains(text(),'기간')]"\
                "/following::div[contains(@class,'_calendar_select_layer')]"
            self.click_element(set_term_xpath+"//a[contains(@class,'_start_trigger')]", 2)# 시작일 선택 클릭.
            self.click_element(set_term_xpath+"/div[contains(@class,'select_wrap')]" # 연 선택.
                "/div[1]//ul[contains(@class,'lst_item')]/li[contains(@data-value,'"+self.start_date_arr[0]+"')]", 2)
            self.click_element(set_term_xpath+"/div[contains(@class,'select_wrap')]" # 월 선택
                "/div[2]//ul[contains(@class,'lst_item')]/li[contains(@data-value,'" + str(int(self.start_date_arr[1])) + "')]", 2)
            self.click_element(set_term_xpath+"/div[contains(@class,'select_wrap')]"# 일 선택.
                "/div[3]//ul[contains(@class,'lst_item')]/li[contains(@data-value,'" + str(int(self.start_date_arr[2])) + "')]", 2)

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
                set_term_xpath+"/div[contains(@class,'btn_area')]/button", 2
            )
            self.click_element(# 최신순 선택.
                "//div[@id='snb']/div[contains(@class,'api_group_option_sort')]/ul[contains(@class,'lst_option')]"
                "//strong[contains(@class,'tit') and contains(text(),'정렬')]"
                "/following::div[contains(@class,'option')]/a[contains(text(),'최신순')]", 2
            )

            while True:
                paging_allow = self.driver.find_element_by_xpath(
                    "//div[@id='main_pack']/div[contains(@class,'api_sc_page_wrap')]//a[contains(@class,'btn_next')]"
                )
                if paging_allow.get_attribute("aria-disabled") == "true":
                    break

                news_count = 1
                # 뉴스 상세 페이지 이동.
                for i in range(10):
                    temp_tag = self.driver.find_elements_by_xpath(
                        "//li[@id='sp_nws"+str(news_count)+"']//div[contains(@class,'news_info')]/div[contains(@class,'info_group')]/a"
                    )
                    if len(temp_tag)==2:
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

                        press = self.driver.find_element_by_xpath(
                            "//div[@id='main_content']/div[contains(@class,'article_header')]"
                            "/div[contains(@class,'press_logo')]/a/img"
                        ).get_attribute("title")
                        title = self.driver.find_element_by_xpath("//h3[@id='articleTitle']").text
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
                            "/span[contains(@class,'num')]"
                        ).text
                        comment_count = self.driver.find_element_by_xpath(
                            "//a[@id='articleTitleCommentCount']/span[contains(@class,'lo_txt')]"
                        ).text
                        emotion_like_cnt = self.driver.find_element_by_xpath(
                            "//div[@id='spiLayer']/div[contains(@class,'_reactionModule')]/ul"
                            "/li[contains(@class,'good')]//span[contains(@class,'u_likeit_list_count')]"
                        ).text
                        emotion_warm_cnt = self.driver.find_element_by_xpath(
                            "//div[@id='spiLayer']/div[contains(@class,'_reactionModule')]/ul"
                            "/li[contains(@class,'warm')]//span[contains(@class,'u_likeit_list_count')]"
                        ).text
                        emotion_sad_cnt = self.driver.find_element_by_xpath(
                            "//div[@id='spiLayer']/div[contains(@class,'_reactionModule')]/ul"
                            "/li[contains(@class,'sad')]//span[contains(@class,'u_likeit_list_count')]"
                        ).text
                        emotion_angry_cnt = self.driver.find_element_by_xpath(
                            "//div[@id='spiLayer']/div[contains(@class,'_reactionModule')]/ul"
                            "/li[contains(@class,'angry')]//span[contains(@class,'u_likeit_list_count')]"
                        ).text
                        emotion_want_cnt = self.driver.find_element_by_xpath(
                            "//div[@id='spiLayer']/div[contains(@class,'_reactionModule')]/ul"
                            "/li[contains(@class,'want')]//span[contains(@class,'u_likeit_list_count')]"
                        ).text


                        self.driver.close()
                        self.driver.switch_to.window(self.driver.window_handles[0])

                        news_count = news_count + 1
                    else:
                        news_count = news_count + 1
                        continue





        # 기간 검색 조건 배열 생성
        start_date = datetime.date.fromisoformat(self.start_date)
        end_date = datetime.date.fromisoformat(self.end_date)

        input_start_date = start_date
        while True:
            try:
                # 이미 스크랩한 리포트 날짜 목록.
                stored_date_list = pd.read_sql("select date, code, title from report_summary ",
                                                    self.db).sort_values(by="date")
                stored_date_list = stored_date_list.astype("string")

                self.driver.find_element_by_xpath("//input[@id='inFromDate']").clear()
                self.wait(2)
                self.driver.find_element_by_xpath("//input[@id='inFromDate']").send_keys(input_start_date.isoformat().replace("-", "/"))
                self.wait(2)

                if input_start_date + datetime.timedelta(days=13) >= end_date:
                    # 기간 종료일에 spider가 인자로 받은 종료일 입력.
                    self.driver.find_element_by_xpath("//input[@id='inToDate']").clear()
                    self.wait(2)
                    self.driver.find_element_by_xpath("//input[@id='inToDate']").send_keys(self.end_date.replace("-", "/"))
                    self.wait(2)
                else:
                    # 기간 시작일에서 13일 더한 날짜를 입력.
                    temp_date = input_start_date + datetime.timedelta(days=13)
                    self.driver.find_element_by_xpath("//input[@id='inToDate']").clear()
                    self.wait(2)
                    self.driver.find_element_by_xpath("//input[@id='inToDate']")\
                        .send_keys(temp_date.isoformat().replace("-", "/"))
                    self.wait(2)

                self.click_element("//a[@id='btnSearch']", 5)
                search_count = search_count + 1

                # 리포트 리스트 스크래핑
                report_list = self.driver.find_elements_by_xpath("//tbody[@id='GridBody']/tr")
                report_list_selector = Selector(text=self.driver.page_source)
                report_list = report_list_selector.xpath("//tbody[@id='GridBody']/tr")
                insert_sql = ""
                for report in report_list:
                    try:
                        created_date = "".join(report.xpath("./td[1]//text()").getall()).replace("/", "-")
                        code = report.xpath("./td[2]//dt/a/span/text()").get()
                        stock_name = report.xpath("./td[2]//dt/a/text()").get().strip()
                        report_name = report.xpath("./td[2]//dt/span//text()").get().strip().lstrip("-")
                        # 이미 스크랩한 날짜이면, 패스.
                        if (
                                (created_date == stored_date_list["date"])
                                & (code == stored_date_list["code"])
                                & (report_name == stored_date_list["title"])
                        ).any():
                            continue
                        report_summary = ""
                        for summary in report.xpath("./td[2]//dd/text()").getall():
                            report_summary = report_summary + summary.strip() + "\n"
                        decision = ""
                        if len(report.xpath("./td[3]/span//text()").getall())==2:
                            decision = report.xpath("./td[3]/span//text()").getall()[1].strip()
                        target_price = "null"
                        if len(report.xpath("./td[4]/span//text()").getall()) == 2:
                            target_price = report.xpath("./td[4]/span//text()").getall()[1].replace(",","").strip()
                        current_price = report.xpath("./td[5]//text()").get().replace("\xa0;", "").replace(",","").strip()
                        temp_str = report.xpath("./td[6]/span//text()").getall()
                        fin_corp = ""
                        writer_name = ""
                        if len(temp_str)==1:
                            fin_corp = report.xpath("./td[6]/span//text()").getall()[0].strip()
                        else:
                            fin_corp = report.xpath("./td[6]/span//text()").getall()[0].strip()
                            writer_name = report.xpath("./td[6]/span//text()").getall()[1].strip()

                        insert_sql = insert_sql + ", ("\
                            "'" + str(datetime.datetime.now(datetime.timezone.utc)) + "', "\
                            "'" + str(datetime.datetime.now(datetime.timezone.utc)) + "', "\
                            "'" + stock_name + "', '" + report_name + "', '" + report_summary + "', '" + decision + "', " + target_price + ", "\
                            "" + current_price + ", '" + fin_corp + "', '" + writer_name + "', '" + created_date + "', '" + code + "')"

                    except Exception as e:
                        self.report_error(e, code, stock_name)
                        continue

                if insert_sql != "":
                    try:
                        self.cur.execute("insert into report_summary ("
                                         "  created_at, updated_at, stock_name, title, summary, decision, target_price, "
                                         "  current_price, fin_corp, writer, date, code"
                                         ") values "+insert_sql[1:])
                        self.db.commit()
                    except Exception as e:
                        self.db.rollback()
                        raise Exception

                # 크롬 메모리 out 방지 및 검색 횟수 제한
                if (search_count % 30 == 0) :
                    self.driver.quit()
                    self.wait(4)
                    self.initial_setting()
                # 검색 회수 제한.
                self.wait(1, search_count=search_count)

                input_start_date = input_start_date + datetime.timedelta(days=14)
                if input_start_date > end_date:
                    break
                else:
                    continue

            except NoSuchWindowException as e:
                self.report_error(e, code, stock_name)

                self.driver.quit()
                self.wait(4)
                self.initial_setting()

                continue

            except Exception as e:
                self.report_error(e, code, stock_name)
                continue

    def report_error(self, e, code, stock_name):
        date_time = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time()))
        with open(constant.error_file_path + "/fnguide_report_summary_error_list_" +
                  time.strftime("%Y-%m-%d", time.localtime(time.time())) + ".txt", "a", encoding="UTF-8") as f:
            f.write(date_time + "_"+code+"_"+stock_name+"\n")
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
            self.wait(wait_time)
        else:
            self.wait(wait_time, term)

    def wait(self, wait_time,  term=5, search_count=None):
        now = datetime.datetime.now()
        # 검색 쿼리 횟수 제한
        if (search_count!=None) :
            if(search_count >= self.scraping_count_goal):
                while datetime.datetime.now() > datetime.datetime(now.year, now.month, now.day+1, 6):
                    time.sleep(10)
                else:
                    self.start_time = self.start_time + datetime.timedelta(days=1)
                    self.start_time = self.start_time.replace(hour=int(random.triangular(9, 10, 9)),
                                                              minute=int(random.randrange(0, 59, 1)))
                    self.break_time = self.break_time + datetime.timedelta(days=1)
                    self.break_time = self.break_time.replace(hour=int(random.triangular(12, 13, 13)),
                                                              minute=int(random.randrange(0, 59, 1)))
                    self.end_time = self.end_time + datetime.timedelta(days=1)
                    self.end_time = self.end_time.replace(hour=int(random.triangular(5, 7, 6)),
                                                          minute=int(random.randrange(0, 59, 1)))

        # 시작시간, 중간 쉬는 시간, 끝시간에 따른 대기.
        if (self.start_time.day == now.day) & (self.start_time > now):
            while self.start_time > now:
                time.sleep(10)
            else:
                self.start_time = self.start_time + datetime.timedelta(days=1)
                self.start_time = self.start_time.replace(hour=int(random.triangular(9,10,9)), minute=int(random.randrange(0,59,1)))

        elif (self.break_time.day == now.day) & (self.break_time < now) & (self.end_time > now):
            # time.sleep(random.normalvariate(3000, 300))
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
                # chrome_options.add_experimental_option("prefs", {
                    # "download.default_directory": constant.download_path.replace("/", "\\") + "\\sometrend",
                    # "profile.content_settings.exceptions.automatic_downloads.*.setting": 1,
                    # "plugins.always_open_pdf_externally": True
                # })
                self.driver = webdriver.Chrome(chrome_driver, chrome_options=chrome_options)
                # self.driver.set_window_position(1300,0)
                self.driver.get('https://www.naver.com/')
                self.driver.implicitly_wait(5)
                self.wait(2)

                # 네이버 뉴스 검색 결과 페이지 이동
                self.driver.find_element_by_xpath("//input[@id='query']").send_keys("코스피")
                self.wait(2)
                self.click_element("//button[@id='search_btn']", 2)

                self.click_element("//div[@id='lnb']/div[contains(@class,'lnb_group')]//ul[contains(@class,'base')]"
                                   "//a[contains(@class,'tab') and contains(text(),'뉴스')]", 2)

            except Exception as e:
                date_time = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time()))
                with open(constant.error_file_path + "/fnguide_report_summary_error_list_" + time.strftime("%Y-%m-%d", time.localtime(
                            time.time())) + ".txt",
                        "a", encoding="UTF-8") as f:
                    f.write(date_time + "_초기 세팅 실패.\n")
                    f.write(traceback.format_exc())
                self.driver.quit()
                self.wait(5+i)
                continue
            else:
                break



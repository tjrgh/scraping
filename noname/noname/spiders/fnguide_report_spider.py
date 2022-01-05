import os
import random
import re
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
from . import common_util as cm

SELENIUM_DRIVER_NAME = 'chrome'
SELENIUM_DRIVER_EXECUTABLE_PATH = which('geckodriver')
SELENIUM_DRIVER_ARGUMENTS=['--headless']  # '--headless' if using chrome instead of firefox


class FnguideReportSpider(scrapy.Spider):
    """
    comp.fnguide.com 사이트의 '요약리포트'정보를 스크래핑하는 스파이더.
    시작 기간과 종료 기간을 인자로 넘기면, 페이지에서 해당 기간 안의 발표된 리포트 목록을 스크래핑하여 'report_summary' table에 저장.
    """

    name = "fnguide_report_summary_spider";

    def __init__(self, start_date, end_date, scraping_count_goal=350):
        super(FnguideReportSpider, self).__init__()
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

        # 시작시간, 중간 쉬는 시간, 종료시간 설정.
        today = time.localtime(time.time())
        self.start_time= datetime.datetime(today.tm_year, today.tm_mon, today.tm_mday, int(random.triangular(9, 10, 9)), int(random.randrange(0, 59, 1)))
        self.break_time = datetime.datetime(today.tm_year, today.tm_mon, today.tm_mday, int(random.triangular(12, 13, 13)), int(random.randrange(0, 59, 1)))
        self.end_time = datetime.datetime(today.tm_year, today.tm_mon, today.tm_mday, int(random.triangular(18, 20, 19)), int(random.randrange(0, 59, 1)))

    def start_requests(self):
        url_list = [
            "https://comp.fnguide.com/SVO2/ASP/SVD_main.asp?pGB=1&gicode=A005930&cID=&MenuYn=Y&ReportGB=&NewMenuID=11&stkGb=&strResearchYN="
        ]

        for url in url_list:
            yield scrapy.Request(url=url)

    def parse(self, response):
        try:
            self.initial_setting()

            self.fnguide_report_summary_scraping()

            self.driver.quit()

        except Exception as e:
            date_time = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time()))
            with open(constant.error_file_path + "/fnguide_report_summary_error_list_" +
                      time.strftime("%Y-%m-%d", time.localtime(time.time())) + ".txt", "a", encoding="UTF-8") as f:
                f.write(date_time + "_전체 에러 \n")
                f.write(traceback.format_exc())

    # 분기별 재무정보 스크래핑 후 업데이트
    def fnguide_report_summary_scraping(self):
        """
        데이터 스크래핑 함수.
        입력받은 기간을 2주간격으로 나누어서 검색하며 스크래핑.
        2주 간격으로 나누는 이유는 그 이상으로 잡을 시, 결과값이 너무 많아서인지 페이지에 결과 출력이 안됨.
        """

        # 기간 검색 조건 배열 생성
        start_date = datetime.date.fromisoformat(self.start_date)
        end_date = datetime.date.fromisoformat(self.end_date)

        input_start_date = start_date
        while True:
            try:
                self.driver.find_element_by_xpath("//input[@id='inFromDate']").clear()
                cm.wait(2)
                self.driver.find_element_by_xpath("//input[@id='inFromDate']").send_keys(input_start_date.isoformat().replace("-", "/"))
                cm.wait(2)

                if input_start_date + datetime.timedelta(days=13) >= end_date:
                    # 기간 종료일에 spider가 인자로 받은 종료일 입력.
                    self.driver.find_element_by_xpath("//input[@id='inToDate']").clear()
                    cm.wait(2)
                    self.driver.find_element_by_xpath("//input[@id='inToDate']").send_keys(self.end_date.replace("-", "/"))
                    cm.wait(2)
                else:
                    # 기간 시작일에서 13일 더한 날짜를 입력.
                    temp_date = input_start_date + datetime.timedelta(days=13)
                    self.driver.find_element_by_xpath("//input[@id='inToDate']").clear()
                    cm.wait(2)
                    self.driver.find_element_by_xpath("//input[@id='inToDate']")\
                        .send_keys(temp_date.isoformat().replace("-", "/"))
                    cm.wait(2)

                self.click_element("//a[@id='btnSearch']", 5)
                self.search_count = self.search_count + 1

                # 리포트 리스트 스크래핑
                report_list = self.driver.find_elements_by_xpath("//tbody[@id='GridBody']/tr")
                report_list_selector = Selector(text=self.driver.page_source)
                report_list = report_list_selector.xpath("//tbody[@id='GridBody']/tr")
                insert_sql = ""
                for report in report_list:
                    try:
                        insert_sql = ""
                        created_date = "".join(report.xpath("./td[1]//text()").getall()).replace("/", "-")
                        code = report.xpath("./td[2]//dt/a/span/text()").get()
                        stock_name = report.xpath("./td[2]//dt/a/text()").get().strip()
                        report_name = report.xpath("./td[2]//dt/span//text()").get().strip().lstrip("-")
                        # 이미 스크랩한 날짜이면, 패스.
                        # 이미 스크랩한 리포트 날짜 목록.
                        stored_date_list = pd.read_sql("select date, code, title from report_summary ",
                                                       self.db).sort_values(by="date")
                        stored_date_list = stored_date_list.astype("string")
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
                        print(insert_sql)
                        try:
                            self.cur.execute("insert into report_summary ("
                                             "  created_at, updated_at, stock_name, title, summary, decision, target_price, "
                                             "  current_price, fin_corp, writer, date, code"
                                             ") values " + insert_sql[1:])
                            self.db.commit()
                        except Exception as e:
                            self.db.rollback()
                            raise Exception

                    except Exception as e:
                        self.report_error(e, code, stock_name)
                        continue

                # if insert_sql != "":
                #     try:
                #         self.cur.execute("insert into report_summary ("
                #                          "  created_at, updated_at, stock_name, title, summary, decision, target_price, "
                #                          "  current_price, fin_corp, writer, date, code"
                #                          ") values "+insert_sql[1:])
                #         self.db.commit()
                #     except Exception as e:
                #         self.db.rollback()
                #         raise Exception

                # 크롬 메모리 out 방지 및 검색 횟수 제한
                if (self.search_count % 30 == 0) :
                    self.driver.quit()
                    cm.wait(4)
                    self.initial_setting()
                # 검색 회수 제한.
                self.search_count = cm.wait(5, search_count=self.search_count, search_count_max=self.scraping_count_goal)["search_count"]

                input_start_date = input_start_date + datetime.timedelta(days=14)
                if input_start_date > end_date:
                    break
                else:
                    continue

            except NoSuchWindowException as e:
                self.report_error(e, code, stock_name)

                self.driver.quit()
                cm.wait(4)
                self.initial_setting()

                continue

            except Exception as e:
                self.report_error(e, code, stock_name)
                continue

        self.driver.quit();

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
            cm.wait(wait_time)
        else:
            cm.wait(wait_time, term)

    def wait(self, wait_time,  term=5, search_count=None):
        now = datetime.datetime.now()
        # 검색 쿼리 횟수 제한
        if (search_count!=None) & (search_count >= self.scraping_count_goal):
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
        if random_value % 2 == 0:
            time.sleep(random.triangular(wait_time, wait_time + term + 5, wait_time + term))
        time.sleep(random.triangular(wait_time, wait_time + term, wait_time))
        # 랜덤 3~5분 대기.
        random_value3 = random.randrange(1, 100, 1)
        if random_value3 % 20 == 0:
            time.sleep(random.uniform(180, 300))
        # 랜덤 10~20분 대기.
        # random_value2 = random.randrange(1, 1000, 1)
        # if random_value2 % 500 == 0:
        #     time.sleep(random.uniform(600, 1200))

    def initial_setting(self):
        for i in range(10):
            try:
                # driver 실행.
                chrome_driver = constant.chrome_driver_path
                chrome_options = Options()
                # chrome_options.add_argument("--headless")
                # chrome_options.add_argument("--no-sandbox")
                # chrome_options.add_argument("--single-process")
                # chrome_options.add_argument("--disable-dev-shm-usage")
                # chrome_options.add_experimental_option("prefs", {
                    # "download.default_directory": constant.download_path.replace("/", "\\") + "\\sometrend",
                    # "profile.content_settings.exceptions.automatic_downloads.*.setting": 1,
                    # "plugins.always_open_pdf_externally": True
                # })
                self.driver = webdriver.Chrome(chrome_driver, chrome_options=chrome_options)
                # self.driver.set_window_position(1300,0)
                self.driver.get('https://comp.fnguide.com/SVO2/ASP/SVD_main.asp?'
                                'pGB=1&gicode=A005930&cID=&MenuYn=Y&ReportGB=&NewMenuID=11&stkGb=&strResearchYN=')
                self.driver.implicitly_wait(5)
                cm.wait(2)

                # 요약리포트 이동.
                self.click_element(
                    "//div[@id='compGnb']//a[contains(@class,'dp1a') and contains(text(),'리포트')]", 2
                )
                self.click_element(
                    "//div[@id='compGnb']//a[contains(@class,'dp1a') and contains(text(),'리포트')]"
                    "/following::a[contains(text(),'요약리포트')]", 2
                )

            except Exception as e:
                date_time = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time()))
                with open(constant.error_file_path + "/fnguide_report_summary_error_list_" + time.strftime("%Y-%m-%d", time.localtime(
                            time.time())) + ".txt",
                        "a", encoding="UTF-8") as f:
                    f.write(date_time + "_초기 세팅 실패.\n")
                    f.write(traceback.format_exc())
                self.driver.quit()
                cm.wait(5+i)
                continue
            else:
                break



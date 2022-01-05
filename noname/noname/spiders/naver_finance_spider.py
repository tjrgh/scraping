
import random

import scrapy
import datetime
import time
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from shutil import which
import pymongo

SELENIUM_DRIVER_NAME = 'chrome'
SELENIUM_DRIVER_EXECUTABLE_PATH = which('geckodriver')
SELENIUM_DRIVER_ARGUMENTS=['--headless']  # '--headless' if using chrome instead of firefox

class NaverFinanceSpider(scrapy.Spider):

    name = 'naver_finance'

    def __init__(self):
        super(NaverFinanceSpider, self).__init__()

        # 시작시간, 중간 쉬는 시간, 종료시간 설정.
        self.start_time = datetime.time(int(random.triangular(9, 10, 9)),
                                        int(random.randrange(0, 59, 1)))
        self.break_time = datetime.time(int(random.triangular(12, 13, 13)),
                                        int(random.randrange(0, 59, 1)))
        self.end_time = datetime.time(int(random.triangular(17, 19, 18)),
                                      int(random.randrange(0, 59, 1)))
        self.search_count = 0
        self.scraping_count_goal = 3

    def start_requests(self):
        urls = [
            'https://finance.naver.com/'
        ]

        for url in urls:
            # yield SeleniumRequest(url = url, callback=self.parse)
            yield scrapy.Request(url=url)

    def parse(self, response):

        while True:
            for i in range(50):
                self.wait(30)
            print(str(datetime.datetime.now()) + "항목 하나 종료.")
            self.search_count = self.search_count + 1
            self.wait(1, search_count=self.search_count)

    def wait(self, wait_time, term=5, search_count=None, search_count_max=None):
        result = {"search_count":search_count}

        # 해당 날짜마다 다른 랜덤 대기 시간 생성.
        today_seed = datetime.date.today().year * datetime.date.today().month * datetime.date.today().day
        random.seed(today_seed)
        start_time = datetime.time(int(random.triangular(9, 10, 9)), int(random.randrange(0, 59, 1)))
        break_time = datetime.time(int(random.triangular(12, 13, 13)), int(random.randrange(0, 59, 1)))
        end_time = datetime.time(int(random.triangular(17, 19, 18)), int(random.randrange(0, 59, 1)))

        now = datetime.datetime.now().time()
        # 검색 쿼리 횟수 제한
        if (search_count != None):
            if (search_count >= search_count_max):
                print("----------------------------------\n")
                print(str(datetime.datetime.now()) + "search count limit break term start.")
                # self.report_error(msg="search count limit break term start.")
                while (datetime.datetime.now().time() < datetime.time(6, 0)) | \
                    (datetime.datetime.now().time() > datetime.time(7, 0)):
                    time.sleep(10)
                else:
                    print("----------------------------------\n")
                    print(str(datetime.datetime.now()) + "search count limit break term end.")
                    # self.report_error(msg="search count limit break term end.")
                    result["search_count"] = 0

        # 시작시간, 중간 쉬는 시간, 끝시간에 따른 대기.
        if (start_time >= now) | (end_time <= now):
            print("----------------------------------\n")
            if start_time >= now:
                print(str(datetime.datetime.now()) + "start break term start.")
            elif end_time <= now:
                print(str(datetime.datetime.now()) + "end break term start.")
            # self.report_error(msg="start break term start.")
            while (start_time >= datetime.datetime.now().time()) | \
                    (end_time <= datetime.datetime.now().time()):
                time.sleep(10)
            else:
                print("----------------------------------\n")
                print(str(datetime.datetime.now()) + "start/end break term end.")
                # self.report_error(msg="start break term end.")
                result["search_count"] = 0
                print((str(datetime.datetime.now()) + "break_time : " + str(self.break_time)))
                print((str(datetime.datetime.now()) + "start_time : " + str(self.start_time)))
                print((str(datetime.datetime.now()) + "end_time : " + str(self.end_time)))

        elif (break_time < now) & \
                ((datetime.datetime.combine(datetime.date.today(), break_time)
                 + datetime.timedelta(minutes=30)).time() > now):
            print("----------------------------------\n")
            print(str(datetime.datetime.now()) + "middle break term start.")
            # self.report_error(msg="middle break term start.")
            time.sleep(random.normalvariate(3000, 400))
            print("----------------------------------\n")
            print(str(datetime.datetime.now()) + "middle break term end.")
            # self.report_error(msg="middle break term end.")

        # 랜덤 몇 초 더 대기.
        random_value = random.randrange(1, 100, 1)
        if random_value % 20 == 0:
            print(str(datetime.datetime.now()) + "more sleep...")
            time.sleep(random.triangular(wait_time, wait_time + term + 10, wait_time + term + 5))
        print(str(datetime.datetime.now()) + "...")
        time.sleep(random.triangular(wait_time, wait_time + term, wait_time))
        # 랜덤 3~5분 대기.
        random_value3 = random.randrange(1, 100, 1)
        if random_value3 % 100 == 0:
            print(str(datetime.datetime.now()) + "3~5minute sleep")
            # self.report_error(msg="3~5minute sleep")
            time.sleep(random.uniform(180, 300))
        # 랜덤 10~20분 대기.
        random_value2 = random.randrange(1, 1000, 1)
        if random_value2 % 500 == 0:
            print(str(datetime.datetime.now()) + "10~20minute sleep")
            # self.report_error(msg="10~20minute sleep")
            time.sleep(random.uniform(600, 1200))

        return result

    # def wait(self, wait_time, term=5):
    #     # 시작시간, 중간 쉬는 시간, 끝시간에 따른 대기.
    #     now = datetime.datetime.now()
    #     if (self.start_time.day == now.day) & (self.start_time > now):
    #         print("시작 대기 시작" + str(datetime.datetime.now()))
    #         while self.start_time > now:
    #             time.sleep(10)
    #         else:
    #             print("시작 대기 종료" + str(datetime.datetime.now()))
    #             self.start_time = self.start_time + datetime.timedelta(days=1)
    #             self.start_time = self.start_time.replace(hour=int(random.triangular(9, 10, 9)), minute=int(random.randrange(0, 59, 1)))
    #             print(str(datetime.datetime.now())+" start_time : "+str(self.start_time))
    #
    #     elif (self.break_time.day == now.day) & (self.break_time < now) & (self.end_time > now):
    #         print("중간 대기 시작" + str(datetime.datetime.now()))
    #         time.sleep(random.normalvariate(3000, 300))
    #         print("중간 대기 종료" + str(datetime.datetime.now()))
    #         self.break_time = self.break_time + datetime.timedelta(days=1)
    #         self.break_time = self.break_time.replace(hour=int(random.triangular(12, 13, 13)),
    #                                         minute=int(random.randrange(0, 59, 1)))
    #         print(str(datetime.datetime.now()) + " break_time : " + str(self.break_time))
    #
    #     elif (self.end_time.day == now.day) & (self.end_time < now):
    #         print("종료 대기 시작" + str(datetime.datetime.now()))
    #         while datetime.datetime.now() < datetime.datetime(now.year, now.month, now.day + 1, 6):
    #             time.sleep(10)
    #         print("종료 대기 종료" + str(datetime.datetime.now()))
    #         self.end_time = self.end_time + datetime.timedelta(days=1)
    #         self.end_time = self.end_time.replace(hour=int(random.triangular(18, 20, 19)),
    #                                     minute=int(random.randrange(0, 59, 1)))
    #         print(str(datetime.datetime.now()) + " end_time : " + str(self.end_time))
    #
    #     # 랜덤 몇 초 더 대기.
    #     random_value = random.randrange(1, 100, 1)
    #     if random_value % 20 == 0:
    #         time.sleep(random.triangular(wait_time, wait_time + term + 5, wait_time + term))
    #     else:
    #         time.sleep(random.triangular(wait_time, wait_time + term, wait_time))
    #     print(str(datetime.datetime.now())+"...")
    #     # 랜덤 3~5분 대기.
    #     random_value3 = random.randrange(1, 100, 1)
    #     if random_value3 % 100 == 0:
    #         print("랜덤 3~5분 대기 시작" + str(datetime.datetime.now()))
    #         time.sleep(random.uniform(180, 300))
    #         print("랜덤 3~5분 대기 종료" + str(datetime.datetime.now()))
    #     # 랜덤 10~20분 대기.
    #     random_value2 = random.randrange(1, 1000, 1)
    #     if random_value2 % 500 == 0:
    #         print("랜덤 10~20분 대기 시작" + str(datetime.datetime.now()))
    #         time.sleep(random.uniform(600, 1200))
    #         print("랜덤 10~20분 대기 종료" + str(datetime.datetime.now()))
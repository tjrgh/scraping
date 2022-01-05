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
import datetime
import time
# from scrapy_selenium import SeleniumRequest
from scrapy import Selector
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, NoSuchWindowException, ElementNotInteractableException, ElementClickInterceptedException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import pymongo
from . import constant_var as constant
from . import common_util as cm

SELENIUM_DRIVER_NAME = 'chrome'
SELENIUM_DRIVER_EXECUTABLE_PATH = which('geckodriver')
SELENIUM_DRIVER_ARGUMENTS=['--headless']  # '--headless' if using chrome instead of firefox

# 분기 종료일?을 입력받아 해당 분기의 데이터를 스크래핑하는 스파이더.
class DeepSearchStockInfoSpider(scrapy.Spider):
    name = "deepsearch_stock_info_spider";

    def __init__(self, scraping_count_goal, target_date=None):
        super(DeepSearchStockInfoSpider, self).__init__()
        self.target_date = target_date
        self.search_count = 0
        self.scraping_count_goal = int(scraping_count_goal)

        self.db = psycopg2.connect(host="112.220.72.179", dbname="openmetric", user="openmetric",
                              password=")!metricAdmin01", port=2345)
        self.cur = self.db.cursor()
        self.stock_list = pd.read_sql("select * from stocks_basic_info where corp_code!=' '", self.db).sort_values(by="code")

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
            "https://www.deepsearch.com/?auth=login"
        ]

        for url in url_list:
            yield scrapy.Request(url=url)

    def parse(self, response):
        try:
            self.initial_setting()
            self.deepsearch_stock_info_scraping()

            self.driver.quit()
        except Exception as e:
            self.report_error(e, msg="스크래퍼 최상단 에러")

    # 분기별 재무정보 스크래핑 후 업데이트
    def deepsearch_stock_info_scraping(self):
        # 디버깅용
        # self.stock_list = self.stock_list[10:]
        # self.stock_list = self.stock_list[self.stock_list["code"]=="A039610"]

        # 분기 데이터 받아야 하는 리스트 대상으로 한번 반복.
        item_count = 0 #반복시마다 증가하는 카운트.(크롬 out of memory오류 방지를 위해 체크)
        # 임시 항목 리스트에 대해 분기 데이터 추출.
        for index, company in self.stock_list.iterrows():
            search_result = True
            # 종목 검색.
            for try_count in range(3):
                try:
                    # '기업검색'항목 이동
                    menu1_button = self.driver.find_element_by_xpath(
                        "//div[@class='deepsearch-app']/div[contains(@class,'drawer-container-layout')]"
                    "/div[contains(@class,'drawer-container')]/div[contains(@class,'drawer-container-inner')]"
                    "/div[contains(@class,'simplebar-wrapper')]/div[contains(@class,'simplebar-mask')]"
                    "//div[contains(@class,'menu-item-group')][2]/div[contains(@class,'menu-item')][3]")
                    self.driver.execute_script("arguments[0].click();", menu1_button)
                    cm.wait(2)

                    # 통합검색창 기업 단축코드 검색.
                    search_bar = self.driver.find_element_by_xpath(
                        "//input[@id='search-input']"
                    )
                    search_bar.send_keys("")
                    cm.wait(2)
                    search_bar.send_keys(company["code"][1:])
                    # search_bar.send_keys("039610")
                    cm.wait(2)
                    search_bar.send_keys(Keys.RETURN)
                    cm.wait(7)
                    self.search_count = self.search_count + 1

                    if self.check_element(
                        "//div[@id='info-list']/div[contains(@class,'search-company-info-view')]", wait_time=30
                    )==False:
                        search_result = False
                        break
                    button = self.driver.find_element_by_xpath(
                        "//div[@id='drawer-content-layout']//div[contains(@class,'deepsearch-content')]"
                        "//div[@id='info-list']//div[contains(@class,'search-company-info-view')]"
                        "/div[contains(@class,'company-info-header')]/a"
                    )
                    self.driver.execute_script("arguments[0].click();", button)
                    cm.wait(7)

                except Exception as e:
                    self.report_error(e, company["code"], company["name"], msg="종목 검색 오류")
                    continue
                else:
                    break
            if search_result==False:#검색 결과가 없으면 다음 종목.
                continue

            # 딥서치 종목명
            deepsearch_stock_name = self.driver.find_element_by_xpath(
                "//div[@id='drawer-content-layout']//div[contains(@class,'deepsearch-content')]"
                "//div[contains(@class,'company-header')]/div[contains(@class,'company-info')]"
                "/div[contains(@class,'name')]"
            ).text

            pass
            # # 증권사 컨센서스 스크래핑
            pass
            for try_count in range(3):
                try:
                    # 재무정보
                    button = self.driver.find_element_by_xpath(
                        "//div[@id='drawer-content-layout']//div[contains(@class,'deepsearch-content')]" \
                        "//div[contains(@class,'company-header')]/div[contains(@class,'tabs')]" \
                        "/a[contains(text(),'재무')]"
                    )
                    self.driver.execute_script("arguments[0].click();", button)
                    cm.wait(7)

                    # 증권사 컨센서스 항목 존재하는지 체크.
                    selector = Selector(text=self.driver.page_source).xpath(
                        "//div[@id='drawer-content-layout']//div[contains(@class,'company-main-page')]"
                        "/div[contains(@class,'company-forecase-financial-view')]"
                    )
                    if len(selector) == 0:
                        break

                    # 연결,개별 확인.
                    com_sep = ""
                    com_sep_xpath = "//div[@id='drawer-content-layout']//div[contains(@class,'company-main-page')]"\
                        "/div[contains(@class,'company-forecase-financial-view')]"\
                        "/div[contains(@class,'header-text')]/div[contains(@class,'header')]/div[contains(@class,'right')]"\
                        "//div[contains(@class,'financial-option')]"
                    com_sep_list = self.driver.find_elements_by_xpath(com_sep_xpath)
                    for com_sep_index in range(1, len(com_sep_list)+1, 1):
                        self.click_element(com_sep_xpath+"["+str(com_sep_index)+"]/input[contains(@id,'ds-radio')]", 5)
                        try:
                            WebDriverWait(self.driver, 5).until(
                                EC.visibility_of_element_located((By.XPATH,
                                  "//div[@id='drawer-content-layout']//div[contains(@class,'company-main-page')]"
                                  "/div[contains(@class,'company-forecase-financial-view')]/div[contains(@class,'financial-container')]"
                                  "/div[contains(@class,'table-content')]//div[contains(@class,'rt-noData')]"
                                ))
                            )
                            cm.wait(2)
                            continue
                        except Exception as e:
                            com_sep = self.driver.find_element_by_xpath(
                                com_sep_xpath+"[" + str(com_sep_index) + "]/input[contains(@id,'ds-radio')]"
                            ).get_attribute("label")
                            break

                    # 추정 년도 확인.
                    selector = selector[0]
                    target_year_list = selector.xpath(
                        "./div[contains(@class,'financial-container')]/div[contains(@class,'slider')]"
                        "//div[contains(@class,'rc-slider-mark')]//span[contains(@class,'date')]"
                    )
                    # 추정 년도 리스트에 대해 반복.
                    for i in range(1, len(target_year_list)+1, 1):
                        target_year_xpath = ""\
                        "//div[@id='drawer-content-layout']//div[contains(@class,'company-main-page')]"\
                        "/div[contains(@class,'company-forecase-financial-view')]"\
                        "/div[contains(@class,'financial-container')]/div[contains(@class,'slider')]"\
                        "//div[contains(@class,'rc-slider-mark')]" \
                        "/span[contains(@class,'rc-slider-mark-text')]["+str(i)+"]/span[contains(@class,'date')]"
                        # 추정 년도 클릭.
                        target_year = self.driver.find_element_by_xpath(target_year_xpath).text
                        self.click_element(target_year_xpath, 2)

                        # 컨센서스 데이터 스크래핑
                        selector = Selector(text=self.driver.page_source).xpath(
                            "//div[@id='drawer-content-layout']//div[contains(@class,'company-main-page')]"
                            "/div[contains(@class,'company-forecase-financial-view')]"
                        )
                        row_list_selector = selector.xpath(
                            "./div[contains(@class,'financial-container')]/div[contains(@class,'table-content')]"
                            "//div[contains(@class,'rt-tbody')]//div[contains(@class,'rt-tr ')]"
                        )
                        # 증권사 추정 목록에 대해 반복.
                        row_list_data = []
                        for row in row_list_selector:
                            column_list = row.xpath("./div")
                            row_data = []
                            # 한 row의 컬럼에 대해 반복.
                            for index, column in enumerate(column_list):
                                if index in [4,7,10,13,16,19]:
                                    continue
                                if len(column.xpath("./span"))==0:
                                    row_data.append(column.xpath("./text()").get().replace(",",""))
                                else:
                                    row_data.append(column.xpath("./span/text()").get().replace(",",""))

                            # 데이터들 처리.
                            # 금액 처리
                            for column_index in range(2,8,1):
                                price = row_data[column_index]

                                if price == "-":
                                    row_data[column_index] = "null"
                                    continue

                                # ex) '1.2억'-> '120000000'
                                zeroCount = ""
                                if price.find("조") != -1:
                                    zeroCount = "12"
                                elif price.find("억") != -1:
                                    zeroCount = "8"
                                price = price.split(" ")
                                price = ("{:."+zeroCount+"f}").format(float(price[0].replace(",",""))).replace(".","")

                                row_data[column_index] = price

                            for column_index in range(8,14,1):
                                price = row_data[column_index]

                                if price == "-":
                                    row_data[column_index] = "null"
                                    continue

                            row_list_data.append(row_data)

                        # 데이터 저장
                        insert_sql = ""
                        for row in row_list_data:
                            # 데이터 중복 제외
                            stored_data_list = pd.read_sql("select code_id, target_year, fin_corp, estimate_date "
                                                           "from consensus "
                                                           "group by code_id, target_year, fin_corp, estimate_date",
                                                           self.db)
                            if (
                                (stored_data_list["code_id"]==company["code"])
                                & (stored_data_list["target_year"]==target_year)
                                & (stored_data_list["fin_corp"]==row[1])
                                & (stored_data_list["estimate_date"]==datetime.date.fromisoformat(row[0]))
                            ).any():
                                continue

                            account_name=["total_sale", "profit", "net_income", "PER", "PBR", "ROA"]
                            for i in range(6):
                                insert_sql = insert_sql+", ("\
                                "'"+str(datetime.datetime.now(datetime.timezone.utc))+"', "\
                                "'"+str(datetime.datetime.now(datetime.timezone.utc))+"', "\
                                "'"+company["corp_code"]+"', '"+row[0]+"', '"+target_year+"', '"+row[1]+"', "\
                                "'"+com_sep+"', '"+account_name[i]+"', "+row[2*i+2]+", "+row[2*i+3]+", '"+company["code"]+"'"\
                                ")"

                        if insert_sql != "":
                            try:
                                self.cur.execute("insert into consensus ("
                                                 "created_at, updated_at, corp_code, estimate_date, target_year, fin_corp, "
                                                 "com_sep, account_name, lastest_estimate, before_estimate, code_id)"
                                                 "values "+insert_sql[1:])
                                self.db.commit()
                            except Exception as e:
                                self.db.rollback()
                                self.report_error(e, company["code"], company["name"], msg="db insert error")

                except NoSuchWindowException as e:
                    self.restart_chrome_driver()
                    self.report_error(e, code=company["code"], stock_name=company["name"])
                    continue
                except Exception as e:
                    self.report_error(e, code=company["code"], stock_name=company["name"])
                    continue

            pass
            # # 주주 정보, 배당 정보 스크래핑
            pass
            for try_count in range(3):
                try:
                    self.click_element(
                        "//div[@id='drawer-content-layout']//div[contains(@class,'deepsearch-content')]" \
                        "//div[contains(@class,'company-header')]/div[contains(@class,'tabs')]" \
                        "/a[contains(text(),'주주 / 배당')]", 7
                    )

                    if self.check_element(
                        "//div[contains(@class,'company-shareholders')]/div[contains(@class,'content')]"
                        "/div[contains(@class,'table-content')]/div[contains(@class,'react-table-layout')]"
                        "//span[contains(@class,'table-export-button')]"
                    ) == False:
                        break

                    downloaded_file_path = constant.download_path + "/deepSearch/DeepSearch-" +\
                                               deepsearch_stock_name + "-주주-정보Table-Export.xlsx"
                    # 딥서치 다운 파일명 중복 방지를 위해 다운명 파일 삭제.
                    if os.path.isfile(downloaded_file_path):
                        os.remove(downloaded_file_path)

                    # 주주 정보 엑셀 다운.
                    self.click_element(
                        "//div[contains(@class,'company-shareholders')]/div[contains(@class,'content')]"
                        "/div[contains(@class,'table-content')]/div[contains(@class,'react-table-layout')]"
                        "//span[contains(@class,'table-export-button')]", 4
                    )
                    # 다운 완료 체크
                    if self.wait_file_download(downloaded_file_path, 60) == False:
                        raise Exception("파일 다운 시간 초과")
                    # 파일명 변환.
                    if os.path.isfile(constant.download_path + "/deepSearch/" + str(company["code"][1:])+"_"+str(
                            company["name"]) + "_주주정보.xlsx"):
                        os.remove(constant.download_path + "/deepSearch/" + str(company["code"][1:])+"_"+str(
                            company["name"]) + "_주주정보.xlsx")
                    os.rename(
                        constant.download_path + "/deepSearch/DeepSearch-" + deepsearch_stock_name + "-주주-정보Table-Export.xlsx",
                        constant.download_path + "/deepSearch/" + str(company["code"][1:])+"_"+str(
                            company["name"]) + "_주주정보.xlsx")

                    # 엑셀 파일 db저장.
                    df = pd.read_excel(constant.download_path + "/deepSearch/" + str(company["code"][1:])+"_"+str(
                            company["name"]) + "_주주정보.xlsx")

                    for date_index in range(1, len(df.iloc[0])):
                        df.iloc[0][date_index] = df.iloc[0][date_index].split("T")[0]
                    df.columns = df.iloc[0]
                    df = df.drop([0])

                    stored_data_list = pd.read_sql("select * from shareholder where code_id='"+company["code"]+"'", self.db)
                    # db 저장
                    insert_sql = ""
                    for row_i in df.index:
                        shareholder = df.loc[row_i]["주주명"]
                        # 기간들에 대해 반복
                        for column in df.columns[1:]:
                            share_per = df[column][row_i]
                            if np.isnan(share_per):
                                share_per = "null"

                            # 중복 체크
                            if (
                                (stored_data_list["shareholder_name"]==shareholder)
                                & (stored_data_list["term_name"]==column)
                            ).any():
                                continue

                            insert_sql = insert_sql + ", ("\
                            "'"+str(datetime.datetime.now(datetime.timezone.utc))+"', "\
                            "'"+str(datetime.datetime.now(datetime.timezone.utc))+"', "\
                            "'"+company["code"]+"', '"+company["corp_code"]+"', '"+column+"', '"+shareholder+"', "\
                            ""+str(share_per)+")"

                    if insert_sql != "":
                        try:
                            self.cur.execute(
                                "insert into shareholder(" \
                                "   created_at, updated_at, code_id, corp_code, term_name, shareholder_name, share_per" \
                                ") values "+insert_sql[1:]
                            )
                            self.db.commit()
                        except Exception as e:
                            self.db.rollback()
                            continue

                except Exception as e:
                    self.report_error(e, code=company["code"], stock_name=company["name"])
                    continue
                else:
                    break

            pass
            # # 배당 정보 스크래핑
            pass
            for try_count in range(3):
                try:
                    self.click_element(
                        "//div[@id='drawer-content-layout']//div[contains(@class,'deepsearch-content')]" \
                        "//div[contains(@class,'company-header')]/div[contains(@class,'tabs')]" \
                        "/a[contains(text(),'주주 / 배당')]", 7
                    )

                    if self.check_element(
                        "//div[contains(@class,'company-dividends')]"
                        "/div[contains(@class,'table-content')]/div[contains(@class,'react-table-layout')]"
                        "//span[contains(@class,'table-export-button')]"
                    ) == False:
                        break

                    downloaded_file_path = constant.download_path + "/deepSearch/DeepSearch-" +\
                                               deepsearch_stock_name + "-배당-정보Table-Export.xlsx"
                    # 딥서치 다운 파일명 중복 방지를 위해 다운명 파일 삭제.
                    if os.path.isfile(downloaded_file_path):
                        os.remove(downloaded_file_path)

                    # 배당 정보 엑셀 다운.
                    self.click_element(
                        "//div[contains(@class,'company-dividends')]"
                        "/div[contains(@class,'table-content')]/div[contains(@class,'react-table-layout')]"
                        "//span[contains(@class,'table-export-button')]", 4
                    )
                    # 다운 완료 체크
                    if self.wait_file_download(downloaded_file_path, 60) == False:
                        raise Exception("파일 다운 시간 초과")
                    # 파일명 변환.
                    if os.path.isfile(constant.download_path + "/deepSearch/" + str(company["code"][1:])+"_"+str(
                            company["name"]) + "_배당정보.xlsx"):
                        os.remove(constant.download_path + "/deepSearch/" + str(company["code"][1:])+"_"+str(
                            company["name"]) + "_배당정보.xlsx")
                    os.rename(
                        constant.download_path + "/deepSearch/DeepSearch-" + deepsearch_stock_name + "-배당-정보Table-Export.xlsx",
                        constant.download_path + "/deepSearch/" + str(company["code"][1:])+"_"+str(
                            company["name"]) + "_배당정보.xlsx")

                    # 엑셀 파일 db저장.
                    df = pd.read_excel(constant.download_path + "/deepSearch/" + str(company["code"][1:])+"_"+str(
                            company["name"]) + "_배당정보.xlsx")

                    df.columns = df.iloc[0]
                    df = df.drop([0])

                    stored_data_list = pd.read_sql("select * from dividend where code_id='"+company["code"]+"'", self.db)
                    # db 저장
                    insert_sql = ""
                    for row_i in df.index:
                        account_name = df.loc[row_i]["항목"]
                        # 기간들에 대해 반복
                        for column in df.columns[1:]:
                            value = df[column][row_i]
                            if np.isnan(value):
                                value = "null"

                            # 중복 체크
                            if (
                                (stored_data_list["account_name"]==account_name)
                                & (stored_data_list["term_name"]==column)
                            ).any():
                                continue

                            insert_sql = insert_sql + ", ("\
                            "'"+str(datetime.datetime.now(datetime.timezone.utc))+"', "\
                            "'"+str(datetime.datetime.now(datetime.timezone.utc))+"', "\
                            "'"+company["code"]+"', '"+company["corp_code"]+"', '"+column+"', '"+account_name+"', "\
                            ""+str(value)+")"

                    if insert_sql != "":
                        try:
                            self.cur.execute(
                                "insert into dividend(" \
                                "   created_at, updated_at, code_id, corp_code, term_name, account_name, value" \
                                ") values "+insert_sql[1:]
                            )
                            self.db.commit()
                        except Exception as e:
                            self.db.rollback()
                            continue

                except Exception as e:
                    self.report_error(e, code=company["code"], stock_name=company["name"])
                    continue
                else:
                    break

            pass
            # # 자회사 및 관계사
            pass
            for try_count in range(3):
                try:
                    self.click_element(
                        "//div[@id='drawer-content-layout']//div[contains(@class,'deepsearch-content')]" \
                        "//div[contains(@class,'company-header')]/div[contains(@class,'tabs')]" \
                        "/a[contains(text(),'관계회사 / 사업분야')]", 7
                    )

                    # 데이터 유무 확인.
                    if self.check_element(
                        "//div[contains(@class,'company-related-firms')]/div[contains(@class,'content')]"
                        "/div[contains(@class,'related-firms-table-container')]/div[contains(@class,'react-table-layout')]"
                        "//span[contains(@class,'table-export-button')]"
                    ) == False:
                        break

                    downloaded_file_path = constant.download_path + "/deepSearch/DeepSearch-" +\
                                               deepsearch_stock_name + "-자회사-및-관계사Table-Export.xlsx"
                    # 딥서치 다운 파일명 중복 방지를 위해 다운명 파일 삭제.
                    if os.path.isfile(downloaded_file_path):
                        os.remove(downloaded_file_path)

                    # 자회사 및 관계사 정보 엑셀 다운.
                    self.click_element(
                        "//div[contains(@class,'company-related-firms')]/div[contains(@class,'content')]"
                        "/div[contains(@class,'related-firms-table-container')]/div[contains(@class,'react-table-layout')]"
                        "//span[contains(@class,'table-export-button')]", 4
                    )
                    # 다운 완료 체크
                    if self.wait_file_download(downloaded_file_path, 60) == False:
                        raise Exception("파일 다운 시간 초과")
                    # 파일명 변환.
                    if os.path.isfile(constant.download_path + "/deepSearch/" + str(company["code"][1:])+"_"+str(
                            company["name"]) + "_자회사및관계사정보.xlsx"):
                        os.remove(constant.download_path + "/deepSearch/" + str(company["code"][1:])+"_"+str(
                            company["name"]) + "_자회사및관계사정보.xlsx")
                    os.rename(
                        constant.download_path + "/deepSearch/DeepSearch-" + deepsearch_stock_name + "-자회사-및-관계사Table-Export.xlsx",
                        constant.download_path + "/deepSearch/" + str(company["code"][1:])+"_"+str(
                            company["name"]) + "_자회사및관계사정보.xlsx")

                    # 엑셀 파일 db저장.
                    df = pd.read_excel(constant.download_path + "/deepSearch/" + str(company["code"][1:])+"_"+str(
                            company["name"]) + "_자회사및관계사정보.xlsx")

                    df.columns = df.iloc[0]
                    df = df.drop([0])

                    stored_data_list = pd.read_sql("select * from af_and_sub where code_id='"+company["code"]+"'", self.db)
                    # db 저장
                    insert_sql = ""
                    for row_i in df.index:
                        relative_stock_code = df.loc[row_i]["종목코드"]
                        # 기간들에 대해 반복
                        for column in df.columns[3:]:
                            value = df[column][row_i]
                            if np.isnan(value):
                                value = "null"

                            # 중복 체크
                            if (
                                (stored_data_list["term_name"]==self.target_date)
                                & (stored_data_list["relative_stock_code"]==relative_stock_code)
                                & (stored_data_list["account_name"]==column)
                            ).any():
                                continue

                            insert_sql = insert_sql + ", ("\
                            "'"+str(datetime.datetime.now(datetime.timezone.utc))+"', "\
                            "'"+str(datetime.datetime.now(datetime.timezone.utc))+"', "\
                            "'"+company["code"]+"', '"+company["corp_code"]+"', '"+self.target_date+"', "\
                            "'"+relative_stock_code+"', '"+column+"', "+str(value)+")"

                    if insert_sql != "":
                        try:
                            self.cur.execute(
                                "insert into af_and_sub(" \
                                "   created_at, updated_at, code_id, corp_code, term_name, relative_stock_code, "\
                                "   account_name, value" \
                                ") values "+insert_sql[1:]
                            )
                            self.db.commit()
                        except Exception as e:
                            self.db.rollback()
                            continue

                except Exception as e:
                    self.report_error(e, code=company["code"], stock_name=company["name"])
                    continue
                else:
                    break

            pass
            # # 평균 급여 및 종업원수 데이터 스크래핑
            pass
            for try_count in range(3):
                try:
                    self.click_element(
                        "//div[@id='drawer-content-layout']//div[contains(@class,'deepsearch-content')]" \
                        "//div[contains(@class,'company-header')]/div[contains(@class,'tabs')]" \
                        "/a[contains(text(),'임직원')]", 7
                    )
                    # 데이터 유무 확인.
                    if self.check_element("//div[contains(@class,'dart-company-employee')][2]"
                      "/div[contains(@class,'header-text')]")==False:
                      break

                    # 데이터 기준 분기별로 수정.
                    year_term_button_xpath = "//div[contains(@class,'dart-company-employee')][2]" \
                        "/div[contains(@class,'header-text')]//div[contains(@class,'right')]//input"
                    year_term_button = self.driver.find_element_by_xpath(year_term_button_xpath)
                    if year_term_button.get_attribute("checked")=="true":
                        self.click_element(year_term_button_xpath, 4)
                    # 기간 바에 대해 반복.
                    term_list_xpath = "//div[contains(@class,'dart-company-employee')][2]" \
                        "/div[contains(@class,'hide-track')]/div"
                    term_list = self.driver.find_elements_by_xpath(
                        term_list_xpath+"/div[contains(@class,'rc-slider-mark')]/span")
                    for term_index in range(1, len(term_list)+1, 1):
                    # test_repet_count = 2
                    # if len(term_list) <= 1:
                    #     test_repet_count = 1
                    # for term_index in range(1, 1 + test_repet_count, 1):
                        self.click_element(
                            term_list_xpath+"/div[contains(@class,'rc-slider-step')]/span["+str(term_index)+"]", 1
                        )
                        term_name = self.driver.find_element_by_xpath(
                            term_list_xpath + "/div[contains(@class,'rc-slider-mark')]/span[" + str(term_index) + "]"
                            "//span[contains(@class,'date')]"
                        ).text

                        # 중복 체크
                        stored_data_list = pd.read_sql("select * from worker_count_pay where code_id='" + company["code"] + "'", self.db)
                        if (
                                (stored_data_list["term_name"] == term_name)
                        ).any():
                            continue

                        if self.check_element(
                            "//div[contains(@class,'dart-company-employee')][2]/div[contains(@class,'dart-employee-table')]"
                            "//div[contains(@class,'react-table-layout')]"
                            "//span[contains(@class,'table-export-button')]"
                        ) == False:
                            continue

                        downloaded_file_path = constant.download_path + "/deepSearch/DeepSearch-" +\
                                                   deepsearch_stock_name + "-임원-정보Table-Export.xlsx"
                        # 딥서치 다운 파일명 중복 방지를 위해 다운명 파일 삭제.
                        if os.path.isfile(downloaded_file_path):
                            os.remove(downloaded_file_path)

                        # 종업원수 및 급여평균 엑셀 다운.
                        self.click_element(
                            "//div[contains(@class,'dart-company-employee')][2]/div[contains(@class,'dart-employee-table')]"
                            "//div[contains(@class,'react-table-layout')]"
                            "//span[contains(@class,'table-export-button')]", 4
                        )

                        # 다운 완료 체크
                        if self.wait_file_download(downloaded_file_path, 60) == False:
                            raise Exception("파일 다운 시간 초과")
                        # 파일명 변환.
                        if os.path.isfile(constant.download_path + "/deepSearch/" + str(company["code"][1:])+"_"+str(
                                company["name"]) + "_평균급여및종업원수_"+term_name+".xlsx"):
                            os.remove(constant.download_path + "/deepSearch/" + str(company["code"][1:])+"_"+str(
                                company["name"]) + "_평균급여및종업원수_"+term_name+".xlsx")
                        os.rename(
                            constant.download_path + "/deepSearch/DeepSearch-" + deepsearch_stock_name + "-임원-정보Table-Export.xlsx",
                            constant.download_path + "/deepSearch/" + str(company["code"][1:])+"_"+str(
                                company["name"]) + "_평균급여및종업원수_"+term_name+".xlsx")

                        # 엑셀 파일 db저장.
                        df = pd.read_excel(constant.download_path + "/deepSearch/" + str(company["code"][1:])+"_"+str(
                                company["name"]) + "_평균급여및종업원수_"+term_name+".xlsx")

                        df.columns = df.iloc[0]
                        df = df.drop([0])
                        df = df.loc[:, ~df.columns.duplicated()]

                        # db 저장
                        insert_sql = ""
                        category = ""
                        sex= ""
                        for row_i in df.index:
                            if (df["카테고리"][row_i] != "남") and (df["카테고리"][row_i]!="여"):
                                category = df["카테고리"][row_i]
                                continue
                            elif df["카테고리"][row_i]=="남":
                                sex="m"
                            elif df["카테고리"][row_i]=="여":
                                sex="w"

                            # 항목들에 대해 반복
                            for column_i, column in enumerate(df.columns[1:7]):
                                value = df[column][row_i]

                                # 데이터 전처리
                                if column_i == 3:  # '평균 근속'컬럼에 '**년 **개월'형태의 데이터 있는 경우.
                                    if value.find("월") != -1:
                                        month_word = ""
                                        if value.find("개월") != -1:
                                            month_word = "개월"
                                        else:
                                            month_word = "월"
                                        temp_value = 0
                                        if value.find("년") != -1:
                                            temp_value = float(value.split("년")[0])
                                            temp_value = temp_value + float(value.split("년")[1].split(month_word)[0]) / 12
                                        else:
                                            temp_value = float(value.split(month_word)[0]) / 12
                                        value = temp_value
                                    elif value.find("년") != -1:  # '년'만 포함된 경우
                                        value = float(value.split("년")[0])
                                    elif value.find("年") != -1:# 한자 '연'이 포함된 경우
                                        value = float(value.split("年")[0])
                                    else:
                                        try: #위와 같이 처리 하였으나, 엑셀 파일이 공시 데이터를 그대로 옮겨 놓은 것이므로 예외가 발생하기 마련. 에러 기록하고 null값 저장.
                                            # 이후 에러 로그를 보고 수정.
                                            value = float(value)
                                        except Exception as e:
                                            self.report_error(e, company["code"], company["name"])
                                            value = np.nan;


                                if np.isnan(value):  # nan인 경우.
                                    value = "null"

                                # 중복 체크
                                if (
                                    (stored_data_list["term_name"]==term_name)
                                    & (stored_data_list["worker_category"]==category)
                                    & (stored_data_list["worker_sex"]==sex)
                                    & (stored_data_list["item_name"]==column)
                                ).any():
                                    continue

                                insert_sql = insert_sql + ", ("\
                                "'"+str(datetime.datetime.now(datetime.timezone.utc))+"', "\
                                "'"+str(datetime.datetime.now(datetime.timezone.utc))+"', "\
                                "'"+company["code"]+"', '"+company["corp_code"]+"', '"+term_name+"', "\
                                "'"+category+"', '"+sex+"', '"+column+"', "+str(value)+")"

                        if insert_sql != "":
                            try:
                                self.cur.execute(
                                    "insert into worker_count_pay(" \
                                    "   created_at, updated_at, code_id, corp_code, term_name, worker_category, "\
                                    "   worker_sex, item_name, value" \
                                    ") values "+insert_sql[1:]
                                )
                                self.db.commit()
                            except Exception as e:
                                self.db.rollback()
                                continue

                except Exception as e:
                    self.report_error(e, code=company["code"], stock_name=company["name"])
                    continue
                else:
                    break

            pass
            # 임원 보수 목록
            pass
            for try_count in range(3):
                try:
                    self.click_element(
                        "//div[@id='drawer-content-layout']//div[contains(@class,'deepsearch-content')]" \
                        "//div[contains(@class,'company-header')]/div[contains(@class,'tabs')]" \
                        "/a[contains(text(),'임직원')]", 7
                    )

                    if self.check_element("//div[contains(@class,'dart-company-employee')][1]"
                                          "/div[contains(@class,'hide-track')]")==False:
                        break

                    # 데이터 기준 분기별로 수정.
                    year_term_button_xpath = "//div[contains(@class,'dart-company-employee')][2]" \
                                             "/div[contains(@class,'header-text')]//div[contains(@class,'right')]//input"
                    year_term_button = self.driver.find_element_by_xpath(year_term_button_xpath)
                    if year_term_button.get_attribute("checked") == "true":
                        self.click_element(year_term_button_xpath, 4)
                    # 기간 바에 대해 반복.
                    term_list_xpath = "//div[contains(@class,'dart-company-employee')][1]" \
                                      "/div[contains(@class,'hide-track')]/div"
                    term_list = self.driver.find_elements_by_xpath(
                        term_list_xpath + "/div[contains(@class,'rc-slider-mark')]/span")
                    for term_index in range(1, len(term_list)+1, 1):
                    # test_repet_count = 2
                    # if len(term_list) <= 1:
                    #     test_repet_count = 1
                    # for term_index in range(1, 1+test_repet_count, 1):
                        self.click_element(
                            term_list_xpath + "/div[contains(@class,'rc-slider-step')]/span[" +
                            str(term_index) + "]", 4
                        )
                        term_name = self.driver.find_element_by_xpath(
                            term_list_xpath + "/div[contains(@class,'rc-slider-mark')]/span[" +
                            str(term_index) + "]//span[contains(@class,'date')]"
                        ).text

                        # 중복 체크
                        stored_data_list = pd.read_sql(
                            "select * from executive_wage where code_id='" + company["code"] + "'", self.db)
                        if (
                                (stored_data_list["term_name"] == term_name)
                        ).any():
                            continue

                        if self.check_element(
                            "//div[contains(@class,'dart-company-employee')][1]/div[contains(@class,'dart-employee-table')]"
                            "/div[contains(@class,'react-table-layout')]"
                            "//span[contains(@class,'table-export-button')]"
                        ) == False:
                            continue

                        # 상위 보수 지급자 엑셀 다운.
                        self.click_element(
                            "//div[contains(@class,'dart-company-employee')][1]/div[contains(@class,'dart-employee-table')]"
                            "/div[contains(@class,'react-table-layout')]"
                            "//span[contains(@class,'table-export-button')]", 4
                        )

                        # 다운 완료 체크
                        downloaded_file_path = constant.download_path + "/deepSearch/DeepSearch-" + \
                                               deepsearch_stock_name + "-개인별-상위Table-Export.xlsx"
                        rename_file_path = constant.download_path + "/deepSearch/" + str(company["code"][1:]) + "_" + \
                                           str(company["name"]) + "_개인별상위보수지급자_" + term_name + ".xlsx"

                        # 딥서치 다운 파일명 중복 방지를 위해 다운명 파일 삭제.
                        if os.path.isfile(downloaded_file_path):
                            os.remove(downloaded_file_path)

                        if self.wait_file_download(downloaded_file_path, 60) == False:
                            raise Exception("파일 다운 시간 초과")

                        # 파일명 변환.
                        self.move_file(downloaded_file_path, rename_file_path)

                        # 엑셀 파일 db저장.
                        df = pd.read_excel(rename_file_path)

                        # DataFrame 구조 정리.
                        df.columns = df.iloc[0]
                        df = df.drop([0])
                        df = df.loc[:, ~df.columns.duplicated()]

                        # db 저장
                        insert_sql = ""
                        for row_i in df.index:
                            # 중복 체크
                            if (
                                    (stored_data_list["term_name"] == term_name)
                                    & (stored_data_list["worker_name"] == df["이름"][row_i])
                                    & (stored_data_list["worker_position"] == df["직책"][row_i])
                            ).any():
                                continue

                            insert_sql = insert_sql + ", ("\
                            "'"+str(datetime.datetime.now(datetime.timezone.utc))+"', "\
                            "'"+str(datetime.datetime.now(datetime.timezone.utc))+"', "\
                            "'"+company["code"]+"', '"+company["corp_code"]+"', '"+term_name+"', " \
                            "'"+df["이름"][row_i]+"', '"+df["직책"][row_i]+"', "+str(df["보수"][row_i])+")"

                        if insert_sql != "":
                            try:
                                self.cur.execute(
                                    "insert into executive_wage"
                                    "(created_at, updated_at, code_id, corp_code, term_name, worker_name, "
                                    "   worker_position, wage) values " + insert_sql[1:]
                                )
                                self.db.commit()
                            except Exception as e:
                                self.db.rollback()
                                self.report_error(e, code=company["code"], stock_name=company["name"], msg="db insert error")
                                continue

                except Exception as e:
                    self.report_error(e, code=company["code"], stock_name=company["name"])
                    continue
                else:
                    break
            pass
            # 임직원 숫자 추이
            pass
            for try_count in range(3):
                try:
                    # 상단 '임직원' 탭 클릭.
                    self.click_element(
                        "//div[@id='drawer-content-layout']//div[contains(@class,'deepsearch-content')]" \
                        "//div[contains(@class,'company-header')]/div[contains(@class,'tabs')]" \
                        "/a[contains(text(),'임직원')]", 7
                    )

                    # '임직원 숫자 추이' 엑셀 다운.
                    downloaded_file_path = constant.download_path + "/deepSearch/DeepSearch-Chart-Export.xlsx"
                    rename_file_path = constant.download_path + "/deepSearch/" + str(company["code"][1:]) + "_" + \
                                       str(company["name"]) + "_임직원숫자추이" + ".xlsx"

                    # 엑셀 다운 버튼 없을시, 넘김.
                    if self.check_element(
                        "//div[@id='drawer-content-layout']//div[contains(@class,'deepsearch-content')]" \
                        "//div[contains(@class,'company-employee-view')]/div[contains(@class,'employees-content')]"\
                        "//span[contains(@class,'export-button')]"
                    ) == False:
                        continue

                    # 딥서치 다운 파일명 중복 방지를 위해 다운명 파일 삭제.
                    if os.path.isfile(downloaded_file_path):
                        os.remove(downloaded_file_path)

                    self.click_element(
                        "//div[@id='drawer-content-layout']//div[contains(@class,'deepsearch-content')]" \
                        "//div[contains(@class,'company-employee-view')]/div[contains(@class,'employees-content')]"\
                        "//span[contains(@class,'export-button')]", 4
                    )

                    # 다운 완료 체크
                    if self.wait_file_download(downloaded_file_path, 60) == False:
                        raise Exception("파일 다운 시간 초과")

                    # 파일명 변환.
                    self.move_file(downloaded_file_path, rename_file_path)

                    # 다운 전 중복 체크 없음. 하려면, 다운 받아서 엑셀 파일에서 현재 스크래핑 하려는 분기를 확인해야 함. 해서, 다운 전 중복 확인은 불가.

                    # 엑셀 파일 db저장.
                    df = pd.read_excel(rename_file_path, sheet_name=None)

                    # 중복체크를 위해 현재 기업에 대해 기존 저장된 데이터 불러오기.
                    stored_data_list = pd.read_sql(
                        "select * from staff_number where code_id='" + company["code"] + "'", self.db)

                    # 직원 타입별로 반복하며 db 저장.
                    for staff_type, staff_type_df in df.items():
                        insert_sql = ""
                        for row_i in staff_type_df.index:
                            # 데이터 전처리
                            staff_type_df = staff_type_df.fillna("")

                            # 중복 체크(엑셀 데이터에서 직원 유형, 날짜에 대해)
                            if (
                                    (stored_data_list["term_name"] == staff_type_df["x"][row_i])
                                    & (stored_data_list["staff_type"] == staff_type)
                            ).any():
                                continue

                            # 중복 아니면 insert 쿼리문 생성.
                            insert_sql = insert_sql + ", (" \
                                                      "'" + str(datetime.datetime.now(datetime.timezone.utc)) + "', " \
                                                      "'" + str(datetime.datetime.now(datetime.timezone.utc)) + "', " \
                                                      "'" + company["code"] + "', '" + company["corp_code"] + "', " \
                                                      "'" + staff_type_df["x"][row_i] + "', " \
                                                      "'" + staff_type + "', '"+str(staff_type_df[staff_type][row_i])+"')"

                        # 생성된 insert 쿼리문이 존재하면 db insert.
                        if insert_sql != "":
                            try:
                                self.cur.execute(
                                    "insert into staff_number"
                                    "(created_at, updated_at, code_id, corp_code, term_name, staff_type, "
                                    "   number) values " + insert_sql[1:]
                                )
                                self.db.commit()
                            except Exception as e:
                                self.db.rollback()
                                self.report_error(e, code=company["code"], stock_name=company["name"], msg="db insert error")
                                continue

                except Exception as e:
                    self.report_error(e, code=company["code"], stock_name=company["name"])
                    continue
                else:
                    break
            pass
            # 임원 목록
            pass
            for try_count in range(3):
                try:
                    self.click_element(
                        "//div[@id='drawer-content-layout']//div[contains(@class,'deepsearch-content')]" \
                        "//div[contains(@class,'company-header')]/div[contains(@class,'tabs')]" \
                        "/a[contains(text(),'임직원')]", 7
                    )

                    # 데이터 기준 분기별로 수정.
                    year_term_button_xpath = "//div[contains(@class,'dart-company-employee')][2]" \
                                             "/div[contains(@class,'header-text')]//div[contains(@class,'right')]//input"
                    year_term_button = self.driver.find_element_by_xpath(year_term_button_xpath)
                    if year_term_button.get_attribute("checked") == "true":
                        self.click_element(year_term_button_xpath, 4)

                    # 기간 바에 대해 반복.
                    term_list_xpath = "//div[contains(@class,'company-executives')]" \
                                      "/div[contains(@class,'hide-track')]/div"
                    term_list = self.driver.find_elements_by_xpath(
                        term_list_xpath + "/div[contains(@class,'rc-slider-mark')]/span")
                    for term_index in range(1, len(term_list)+1, 1):
                    # test_repet_count = 2
                    # if len(term_list) <= 1:
                    #     test_repet_count = 1
                    # for term_index in range(1, 1 + test_repet_count, 1):
                        self.click_element(
                            term_list_xpath + "/div[contains(@class,'rc-slider-step')]/span[" +
                            str(term_index) + "]", 1
                        )
                        term_name = self.driver.find_element_by_xpath(
                            term_list_xpath + "/div[contains(@class,'rc-slider-mark')]/span[" +
                            str(term_index) + "]//span[contains(@class,'date')]"
                        ).text

                        # 중복 체크
                        stored_data_list = pd.read_sql(
                            "select * from executives where code_id='" + company["code"] + "'", self.db)
                        if (
                                (stored_data_list["term_name"] == term_name)
                        ).any():
                            continue

                        if self.check_element(
                                "//div[contains(@class,'company-executives')]/div[contains(@class,'company-executives-content')]"
                                "/div[contains(@class,'react-table-layout')]"
                                "//span[contains(@class,'table-export-button')]"
                        ) == False:
                            continue
                        # 임원 목록 엑셀 다운.
                        self.click_element(
                            "//div[contains(@class,'company-executives')]/div[contains(@class,'company-executives-content')]"
                            "/div[contains(@class,'react-table-layout')]"
                            "//span[contains(@class,'table-export-button')]", 4
                        )

                        # 다운 완료 체크
                        downloaded_file_path = constant.download_path + "/deepSearch/DeepSearch-KRX_"+\
                                        company["code"][1:]+"-임원-정보Table-Export.xlsx"
                        rename_file_path = constant.download_path + "/deepSearch/" + str(company["code"][1:]) + "_" + \
                                           str(company["name"]) + "_임원목록_" + term_name + ".xlsx"

                        # 딥서치 다운 파일명 중복 방지를 위해 다운명 파일 삭제.
                        if os.path.isfile(downloaded_file_path):
                            os.remove(downloaded_file_path)

                        if self.wait_file_download(downloaded_file_path, 60) == False:
                            raise Exception("파일 다운 시간 초과")

                        # 파일명 변환.
                        self.move_file(downloaded_file_path, rename_file_path)

                        # 엑셀 파일 db저장.
                        df = pd.read_excel(rename_file_path)

                        # DataFrame 구조 정리.
                        df.columns = df.iloc[0]
                        df = df.drop([0])
                        df = df.loc[:, ~df.columns.duplicated()]

                        # db 저장
                        insert_sql = ""
                        for row_i in df.index:
                            # 데이터 전처리
                            df = df.fillna("")

                            # 중복 체크
                            if (
                                    (stored_data_list["term_name"] == term_name)
                                    & (stored_data_list["worker_name"] == df["이름"][row_i])
                                    & (stored_data_list["worker_birth_date"] == df["생년월일"][row_i])
                                    & (stored_data_list["worker_position"] == df["직책"][row_i])
                            ).any():
                                continue

                            insert_sql = insert_sql + ", ("\
                            "'"+str(datetime.datetime.now(datetime.timezone.utc))+"', "\
                            "'"+str(datetime.datetime.now(datetime.timezone.utc))+"', "\
                            "'"+company["code"]+"', '"+company["corp_code"]+"', '"+term_name+"', " \
                            "'"+df["이름"][row_i]+"', '"+df["직책"][row_i]+"', '"+df["생년월일"][row_i]+"')"

                        if insert_sql != "":
                            try:
                                self.cur.execute(
                                    "insert into executives"
                                    "(created_at, updated_at, code_id, corp_code, term_name, worker_name, "
                                    "   worker_position, worker_birth_date) values " + insert_sql[1:]
                                )
                                self.db.commit()
                            except Exception as e:
                                self.db.rollback()
                                self.report_error(e, code=company["code"], stock_name=company["name"], msg="db insert error")
                                continue

                except Exception as e:
                    self.report_error(e, code=company["code"], stock_name=company["name"])
                    continue
                else:
                    break

            pass
            # 이사회 평균 보수
            pass
            for try_count in range(3):
                try:
                    # 상단 '임직원' 탭 클릭.
                    self.click_element(
                        "//div[@id='drawer-content-layout']//div[contains(@class,'deepsearch-content')]" \
                        "//div[contains(@class,'company-header')]/div[contains(@class,'tabs')]" \
                        "/a[contains(text(),'임직원')]", 7
                    )
                    if year_term_button.get_attribute("checked") == "true":
                        self.click_element(year_term_button_xpath, 4)

                    # '임직원 숫자 추이' 엑셀 다운.
                    downloaded_file_path = constant.download_path + "/deepSearch/DeepSearch-Chart-Export.xlsx"
                    rename_file_path = constant.download_path + "/deepSearch/" + str(company["code"][1:]) + "_" + \
                                       str(company["name"]) + "_이사회평균보수" + ".xlsx"

                    # 엑셀 다운 버튼 없을시, 넘김.
                    if self.check_element(
                        "//div[@id='drawer-content-layout']//div[contains(@class,'deepsearch-content')]" \
                        "//div[contains(@class,'dart-company-employee')]/div[contains(@class,'chart-container')]"\
                        "//span[contains(@class,'export-button')]"
                    ) == False:
                        continue

                    # 딥서치 다운 파일명 중복 방지를 위해 다운명 파일 삭제.
                    if os.path.isfile(downloaded_file_path):
                        os.remove(downloaded_file_path)

                    # 엑셀 다운.
                    self.click_element(
                        "//div[@id='drawer-content-layout']//div[contains(@class,'deepsearch-content')]" \
                        "//div[contains(@class,'dart-company-employee')]/div[contains(@class,'chart-container')]"\
                        "//span[contains(@class,'export-button')]", 4
                    )

                    # 다운 완료 체크
                    if self.wait_file_download(downloaded_file_path, 60) == False:
                        raise Exception("파일 다운 시간 초과")

                    # 파일명 변환.
                    self.move_file(downloaded_file_path, rename_file_path)

                    # 다운 전 중복 체크 없음. 하려면, 다운 받아서 엑셀 파일에서 현재 스크래핑 하려는 분기를 확인해야 함. 해서, 다운 전 중복 확인은 불가.

                    # 엑셀 파일 db저장.
                    df = pd.read_excel(rename_file_path, sheet_name=None)

                    # 중복체크를 위해 현재 기업에 대해 기존 저장된 데이터 불러오기.
                    stored_data_list = pd.read_sql(
                        "select * from board_member_average_wage where code_id='" + company["code"] + "'", self.db)

                    # db 전처리.
                    total_wage_df = df["전체 보수"]
                    total_wage_df.rename(columns={total_wage_df.columns[0]: "term_name"}, inplace=True) # 첫 번째 컬럼명 'term_name'으로 변경.
                    total_wage_df.drop_duplicates(["term_name"], keep="last", inplace=True) # term_name기준으로 중복 row제거. 중복 존재시, 마지막 row만 남김.

                    average_wage_df = df["평균 보수"]
                    average_wage_df.rename(columns={average_wage_df.columns[0]: "term_name"}, inplace=True)
                    average_wage_df.drop_duplicates(["term_name"], keep="last", inplace=True) # term_name기준으로 중복 row제거. 중복 존재시, 마지막 row만 남김.

                    # 기간에 대해 반복하며 db 저장
                    insert_sql = ""
                    for df_idx in total_wage_df.index:
                        term_name = total_wage_df["term_name"][df_idx]
                        total_wage = total_wage_df["전체 보수"][df_idx]
                        average_wage = average_wage_df["평균 보수"][df_idx]

                        # 중복 체크(엑셀 데이터에서 날짜에 대해)
                        if (
                                (stored_data_list["term_name"] == term_name)
                        ).any():
                            continue

                        # 중복 아니면 insert 쿼리문 생성.
                        insert_sql = insert_sql + ", (" \
                            "'" + str(datetime.datetime.now(datetime.timezone.utc)) + "', " \
                            "'" + str(datetime.datetime.now(datetime.timezone.utc)) + "', " \
                            "'" + company["code"] + "', '" + company["corp_code"] + "', " \
                            "'" + term_name + "', " \
                            "" + str(total_wage) + ", "+str(average_wage)+")"

                    # 생성된 insert 쿼리문이 존재하면 db insert.
                    if insert_sql != "":
                        try:
                            self.cur.execute(
                                "insert into board_member_average_wage"
                                "(created_at, updated_at, code_id, corp_code, term_name, total_wage, "
                                "   average_wage) values " + insert_sql[1:]
                            )
                            self.db.commit()
                        except Exception as e:
                            self.db.rollback()
                            self.report_error(e, code=company["code"], stock_name=company["name"], msg="db insert error")
                            continue

                except Exception as e:
                    self.report_error(e, code=company["code"], stock_name=company["name"])
                    continue
                else:
                    break
            pass
            # 이사회 목록 및 이사회 임원 보수 정보.
            for try_count in range(3):
                try:
                    self.click_element(
                        "//div[@id='drawer-content-layout']//div[contains(@class,'deepsearch-content')]" \
                        "//div[contains(@class,'company-header')]/div[contains(@class,'tabs')]" \
                        "/a[contains(text(),'임직원')]", 7
                    )

                    # 데이터 유무 확인
                    if self.check_element("//div[contains(@class,'dart-company-employee')][3]"
                                          "/div[contains(@class,'hide-track')]") == False:
                        break

                    # 데이터 기준 분기별로 수정.
                    year_term_button_xpath = "//div[contains(@class,'dart-company-employee')][2]" \
                                             "/div[contains(@class,'header-text')]//div[contains(@class,'right')]//input"
                    year_term_button = self.driver.find_element_by_xpath(year_term_button_xpath)
                    if year_term_button.get_attribute("checked") == "true":
                        self.click_element(year_term_button_xpath, 4)

                    # 기간 바에 대해 반복.
                    term_list_xpath = "//div[contains(@class,'dart-company-employee')][3]" \
                                      "/div[contains(@class,'hide-track')]/div"
                    term_list = self.driver.find_elements_by_xpath(
                        term_list_xpath + "/div[contains(@class,'rc-slider-mark')]/span")
                    for term_index in range(1, len(term_list) + 1, 1):
                    # test_repet_count = 2
                    # if len(term_list) <= 1:
                    #     test_repet_count = 1
                    # for term_index in range(1, 1 + test_repet_count, 1):
                        self.click_element(
                            term_list_xpath + "/div[contains(@class,'rc-slider-step')]/span[" +
                            str(term_index) + "]", 2
                        )
                        term_name = self.driver.find_element_by_xpath(
                            term_list_xpath + "/div[contains(@class,'rc-slider-mark')]/span[" +
                            str(term_index) + "]//span[contains(@class,'date')]"
                        ).text

                        # 이사회 목록 데이터 저장.
                        # 중복 체크
                        stored_data_list = pd.read_sql(
                            "select * from board_members where code_id='" + company["code"] + "'", self.db)
                        if (
                                (stored_data_list["term_name"] == term_name)
                        ).any()==False:
                            if self.check_element(
                                    "//div[contains(@class,'dart-company-employee')][3]/div[contains(@class,'dart-employee-table')][1]"
                                    "/div[contains(@class,'react-table-layout')]"
                                    "//span[contains(@class,'table-export-button')]"
                            ) == True:
                                # 임원 목록 엑셀 다운.
                                self.click_element(
                                    "//div[contains(@class,'dart-company-employee')][3]/div[contains(@class,'dart-employee-table')][1]"
                                    "/div[contains(@class,'react-table-layout')]"
                                    "//span[contains(@class,'table-export-button')]", 4
                                )

                                # 다운 완료 체크
                                downloaded_file_path = constant.download_path + "/deepSearch/DeepSearch-" + \
                                                       deepsearch_stock_name + "-이사회-임원-목록Table-Export.xlsx"
                                rename_file_path = constant.download_path + "/deepSearch/" + str(company["code"][1:]) + "_" + \
                                                   str(company["name"]) + "_이사회목록_" + term_name + ".xlsx"

                                # 딥서치 다운 파일명 중복 방지를 위해 다운명 파일 삭제.
                                if os.path.isfile(downloaded_file_path):
                                    os.remove(downloaded_file_path)

                                if self.wait_file_download(downloaded_file_path, 60) == False:
                                    raise Exception("파일 다운 시간 초과")

                                # 파일명 변환.
                                self.move_file(downloaded_file_path, rename_file_path)

                                # 엑셀 파일 db저장.
                                df = pd.read_excel(rename_file_path)

                                # DataFrame 구조 정리.
                                df.columns = df.iloc[0]
                                df = df.drop([0])
                                df = df.loc[:, ~df.columns.duplicated()]
                                df = df.drop_duplicates(["이름", "업무", "성별", "생년월일", "상태"])

                                stored_data_list = pd.read_sql(
                                    "select * from board_members where code_id='" + company["code"] + "'", self.db)
                                # db 저장
                                insert_sql = ""
                                for row_i in df.index:
                                    # 전처리
                                    birth_date = ""  # 생년월일
                                    birth_date = df["생년월일"][row_i].replace("년", "-").replace("월", "").replace(" ", "")
                                    office_term = df["임기 기간"][row_i].replace("'", "")
                                    office_end = ""  # 임기 종료
                                    if df["임기 종료"][row_i] != "-":
                                        office_end = df["임기 종료"][row_i]

                                    # 중복 체크
                                    if (
                                            (stored_data_list["term_name"] == term_name)
                                            & (stored_data_list["member_name"] == df["이름"][row_i])
                                            & (stored_data_list["member_birth_date"] == birth_date)
                                            & (stored_data_list["member_job"] == df["업무"][row_i])
                                            & (stored_data_list["member_sex"] == df["성별"][row_i])
                                            & (stored_data_list["member_status"] == df["상태"][row_i])
                                    ).any():
                                        continue

                                    insert_sql = insert_sql + ", (" \
                                        "'" + str(datetime.datetime.now(datetime.timezone.utc)) + "', " \
                                        "'" + str(datetime.datetime.now(datetime.timezone.utc)) + "', " \
                                        "'" + company["code"] + "', '" + company["corp_code"] + "', '" + term_name + "', " \
                                        "'" + df["이름"][row_i] + "', '" + df["업무"][row_i] + "', '" + birth_date + "', "\
                                        "'"+df["성별"][row_i]+"', '"+df["상태"][row_i]+"', '"+office_term+"', "\
                                        "'"+office_end+"'"\
                                        ")"

                                if insert_sql != "":
                                    try:
                                        self.cur.execute(
                                            "insert into board_members"
                                            "("
                                            "   created_at, updated_at, code_id, corp_code, term_name, member_name, "
                                            "   member_job, member_birth_date, member_sex, member_status, "
                                            "   member_office_term, member_office_end"
                                            ") values " + insert_sql[1:]
                                        )
                                        self.db.commit()
                                    except Exception as e:
                                        self.db.rollback()
                                        self.report_error(
                                            e, code=company["code"], stock_name=company["name"], msg="db insert error")
                                        continue

                        # 이사회 임원 보수 정보 데이터 저장.
                        # 다운 전 중복 체크. db에 해당 기간에 대한 데이터 존재시 패스.
                        stored_data_list = pd.read_sql(
                            "select * from board_member_personal_wage where code_id='" + company["code"] + "'", self.db)

                        if (
                                (stored_data_list["term_name"] == term_name)
                        ).any():
                            continue

                        # 엑셀 다운 버튼 유무 확인. 없을시, 데이터 없음.
                        if self.driver.find_element_by_xpath(
                                "//div[contains(@class,'dart-company-employee')][3]/div[contains(@class,'dart-employee-table')][2]"
                                "/div[contains(@class,'react-table-layout')]//span[contains(@class,'table-export-button')]"
                        ).get_attribute("class").find("disable") != -1:
                            continue

                        downloaded_file_path = constant.download_path + "/deepSearch/DeepSearch-" + \
                                               deepsearch_stock_name + "-이사회-임원-보수-정보Table-Export.xlsx"
                        rename_file_path = constant.download_path + "/deepSearch/" + str(company["code"][1:]) + "_" + \
                                           str(company["name"]) + "_이사회임원보수정보_" + term_name + ".xlsx"

                        # 딥서치 다운 파일명 중복 방지를 위해 다운명 파일 삭제.
                        if os.path.isfile(downloaded_file_path):
                            os.remove(downloaded_file_path)

                        # 이사회 임원 보수 정보 엑셀 다운.
                        self.click_element(
                            "//div[contains(@class,'dart-company-employee')][3]/div[contains(@class,'dart-employee-table')][2]"
                                "/div[contains(@class,'react-table-layout')]//span[contains(@class,'table-export-button')]", 4
                        )

                        # 다운 완료 체크
                        if self.wait_file_download(downloaded_file_path, 60) == False:
                            raise Exception("파일 다운 시간 초과")

                        # 파일명 변환.
                        self.move_file(downloaded_file_path, rename_file_path)

                        # 엑셀 파일 db저장.
                        df = pd.read_excel(rename_file_path)

                        # DataFrame 구조 정리.
                        df.columns = df.iloc[0]
                        df = df.drop([0])
                        df = df.loc[:, ~df.columns.duplicated()]
                        # df = df.drop_duplicates(["이름", "업무", "성별", "생년월일", "상태"])

                        # db 저장
                        insert_sql = ""
                        for row_i in df.index:
                            # 중복 체크
                            if (
                                    (stored_data_list["term_name"] == term_name)
                                    & (stored_data_list["member_name"] == df["이름"][row_i])
                                    & (stored_data_list["member_position"] == df["직책"][row_i])
                            ).any():
                                continue

                            insert_sql = insert_sql + ", (" \
                                "'" + str(datetime.datetime.now(datetime.timezone.utc)) + "', " \
                                "'" + str( datetime.datetime.now(datetime.timezone.utc)) + "', " \
                                "'" + company["code"] + "', '" + company["corp_code"] + "', '" + term_name + "', " \
                                "'" + df["이름"][row_i] + "', '" + df["직책"][row_i] + "', " + str(df["보수"][row_i]) + ", " \
                                "'" + df["공시"][row_i] + "')"

                        if insert_sql != "":
                            try:
                                self.cur.execute(
                                    "insert into board_member_personal_wage"
                                    "("
                                    "   created_at, updated_at, code_id, corp_code, term_name, member_name, "
                                    "   member_position, member_wage, notice_code "
                                    ") values " + insert_sql[1:]
                                )
                                self.db.commit()
                            except Exception as e:
                                self.db.rollback()
                                self.report_error(
                                    e, code=company["code"], stock_name=company["name"], msg="db insert error")
                                continue

                except Exception as e:
                    self.report_error(e, code=company["code"], stock_name=company["name"])
                    continue
                else:
                    break

            # 한 종목 스크래핑 종료후, 5~10분 대기.
            self.search_count = cm.wait(60, 60, search_count=self.search_count, search_count_max=self.scraping_count_goal)["search_count"]

    def check_element(self, xpath, condition=EC.visibility_of_element_located, wait_time=5):
        try:
            WebDriverWait(self.driver, wait_time).until(condition((By.XPATH, xpath)))
            cm.wait(3)
            return True
        except Exception as e:
            return False

    def move_file(self, pre_path, path):
        if os.path.isfile(path):
            os.remove(path)
        os.rename(pre_path, path)

    def wait_file_download(self, path, wait_time):
        # 다운 완료 체크
        for i in range(wait_time):
            if os.path.isfile(path):
                return True
            else:
                time.sleep(1)
        else:
            return False

    def store_quarterly_data(self, company, df, subject_name, date, corp_code):

        # dataframe의 컬럼명 변경.
        df.columns = df.loc[0]
        df = df.drop([0])

        # 해당 파일에 대한 데이터 존재하는지 확인.
        self.cur.execute("select * from stock_financial_statement "
                         "where code_id='" + str(company["code"]) + "' and this_term_name='" + date + "' "
                              "and subject_name='" + subject_name + "'")
        if (None != self.cur.fetchone()):  # 이미 해당 데이터가ㅓ 존재한다면,
            date_time = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time()))
            with open(constant.error_file_path + "/quarterly_error_list_" + time.strftime("%Y-%m-%d",
                          time.localtime(time.time())) + ".txt", "a", encoding="UTF-8") as f:
                f.write(date_time + "_" + company["code"][1:] + "_" + company["name"] + "\n")
                f.write(subject_name + "가 이미 존재합니다. \n")
            return ""
        elif (date not in df.columns):
            date_time = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time()))
            with open(constant.error_file_path + "/quarterly_error_list_" + time.strftime("%Y-%m-%d",
                          time.localtime( time.time())) + ".txt", "a", encoding="UTF-8") as f:
                f.write(date_time + "_" + company["code"][1:] + "_" + company["name"] + "\n")
                f.write(subject_name + "에 "+date+"분기 데이터가 존재하지 않습니다.  \n")
            return ""

        # 재무 엑셀 파일에서 누락된 필수 항목들 체크 후, 계산하여 삽입.
        if subject_name == "포괄손익계산서":
            # 주당순이익이 없는 경우.
            if (df["account_id"] == "8200").any() == False:
                account_id = ""
                if (df["account_id"] == "8110").any() == True:
                    account_id = "8110"
                elif (df["account_id"] == "8160").any() == True:
                    account_id = "8160"
                else:
                    raise Exception("총당기순이익 데이터 및 대체 데이터가 존재하지 않습니다.")

                temp_row = df[df["account_id"] == account_id]
                temp_row.iloc[0]["account_id"] = "8200"
                temp_row.iloc[0]["계정명"] = "총당기순이익"
                df.append(temp_row)

        elif subject_name == "재무상태표":
            pass;
        elif subject_name == "현금흐름표":
            pass;

        insert_sql = ""
        for row in df.index:  # 엑셀 파일의 row들에 대해 반복.
            # this_term_amount
            amount = df.loc[row][date]
            if np.isnan(df.loc[row][date]):
                amount = 'null'
            # account_name
            account_name = df.loc[row]["계정명"].replace(" ", "")
            lv = int(df.loc[row]["LV"])
            row_sub_count = 1
            while lv != 0:
                if lv - 1 != int(df.loc[row - row_sub_count]["LV"]):
                    row_sub_count = row_sub_count + 1
                    continue
                lv = int(df.loc[row - row_sub_count]["LV"])
                account_name = df.loc[row - row_sub_count]["계정명"].replace(" ", "") + "_" + account_name
                row_sub_count = row_sub_count + 1

            sql_value = ("(" +
                 "'" + str(datetime.datetime.now(datetime.timezone.utc)) + "', '" + str(datetime.datetime.now(datetime.timezone.utc)) + "', " +
                 "'" + corp_code + "', '" + date.split("-")[0] + "', '" + date.split("-")[1] + "', '" + date + "', '" + subject_name + "', " +
                 "'" + str(df.loc[row]["account_id"]) + "', '" + account_name + "', '" + str(
                df.loc[row]["LV"]) + "', " +
                 "" + str(amount) + ", '" + str(df.loc[row]["LV"]) + "', '" + str(company["code"]) + "')")
            insert_sql = insert_sql + ", " + sql_value

        insert_sql = insert_sql[1:]
        return insert_sql

    def report_error(self, e=None, code="", stock_name="", msg=""):
        date_time = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time()))
        with open(constant.error_file_path + "/deepsearch_stock_info_error_list_" +
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

    def initial_setting(self):
        for i in range(10):
            try:
                # chrome driver 실행.
                chrome_driver = constant.chrome_driver_path
                chrome_options = Options()
                chrome_options.add_experimental_option("prefs", {
                    "download.default_directory": constant.download_path.replace("/", "\\") + "\\deepSearch",
                    "profile.content_settings.exceptions.automatic_downloads.*.setting": 1
                })
                self.driver = webdriver.Chrome(chrome_driver, chrome_options=chrome_options)
                self.driver.get('https://www.deepsearch.com/?auth=login')
                self.driver.implicitly_wait(5)

                # 로그인
                self.driver.find_element_by_xpath(
                    "//div[contains(@class,'login-page')]//div[@class='login-container']//input[@placeholder='계정']").send_keys(
                    "sooryong@gmail.com")
                cm.wait(2+i)
                self.driver.find_element_by_xpath(
                    "//div[contains(@class,'login-page')]//div[@class='login-container']//input[@placeholder='비밀번호']").send_keys(
                    ")!kaimobile01")
                cm.wait(2+i)
                self.driver.find_element_by_xpath(
                    "//div[contains(@class,'login-page')]//div[@class='login-container']//input[@class='button login']").click()
                cm.wait(3+i)
                self.driver.refresh()
                cm.wait(2+i)

                # 메뉴바 클릭.
                menu_bar_button = self.driver.find_element_by_xpath(
                    "//div[@class='deepsearch-appbar']//div[contains(@class,'app-bar-drawer')]")
                self.driver.execute_script("arguments[0].click();", menu_bar_button)
                cm.wait(2+i)

                # '기업검색'항목 이동
                menu1_button = self.driver.find_element_by_xpath(
                    "//div[@class='deepsearch-app']/div[contains(@class,'drawer-container-layout')]"
                    "/div[contains(@class,'drawer-container')]/div[contains(@class,'drawer-container-inner')]"
                    "/div[contains(@class,'simplebar-wrapper')]/div[contains(@class,'simplebar-mask')]"
                    "//div[contains(@class,'menu-item-group')][2]/div[contains(@class,'menu-item')][3]")
                self.driver.execute_script("arguments[0].click();", menu1_button)
                cm.wait(2+i)

            except Exception as e:
                self.report_error(msg="초기 세팅 실패.")
                self.driver.quit()
                cm.wait(2+i)
                continue
            else:
                break



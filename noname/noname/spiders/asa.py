# import copy
# import os
# import random
# import re
# import shutil
# import traceback
# from shutil import which
#
# import numpy as np
# import pandas as pd
# import psycopg2
# import scrapy
# import datetime
# import time
# # from scrapy_selenium import SeleniumRequest
# from selenium import webdriver
# from selenium.common.exceptions import NoSuchElementException, NoSuchWindowException, WebDriverException, \
#     ElementNotInteractableException
# from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.common.keys import Keys
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from selenium.webdriver.common.by import By
# import pymongo
# from . import constant_var as constant
# from . import common_util as cm
#
# SELENIUM_DRIVER_NAME = 'chrome'
# SELENIUM_DRIVER_EXECUTABLE_PATH = which('geckodriver')
# SELENIUM_DRIVER_ARGUMENTS = ['--headless']  # '--headless' if using chrome instead of firefox
#
#
# # 분기 종료일?을 입력받아 해당 분기의 데이터를 스크래핑하는 스파이더.
# class SocialKeywordSpider(scrapy.Spider):
#     name = "social_search_spider";
#
#     def __init__(self):
#         super(SocialKeywordSpider, self).__init__()
#         self.driver = None
#         # self.start_date = start_date
#         # self.start_date_arr = self.start_date.split("-")
#         # self.end_date = end_date
#         # self.end_date_arr = self.end_date.split("-")
#         self.term_type = "F"
#         self.scraping_count_goal = 100
#         self.search_count = 0
#
#         self.db = psycopg2.connect(host="112.220.72.179", dbname="openmetric", user="openmetric",
#                                    password=")!metricAdmin01", port=2345)
#         self.cur = self.db.cursor()
#         # self.keyword_list = pd.read_sql("select * from social_keywords where code_id is null", self.db)
#         self.search_list = pd.read_sql("select * from social_search_list where completed=False", self.db)
#         self.keyword_list = pd.read_sql("select * from social_keywords where is_followed=True", self.db)
#
#     def start_requests(self):
#         url_list = [
#             "https://some.co.kr/"
#         ]
#
#         for url in url_list:
#             yield scrapy.Request(url=url)
#
#     def parse(self, response):
#         try:
#             self.initial_setting()
#             self.social_search_scraping()
#
#         except Exception as e:
#             self.report_error(e, msg="전체에러")
#
#     # 분기별 재무정보 스크래핑 후 업데이트
#     def social_search_scraping(self):
#         # 디버깅용
#         self.search_list = self.search_list[23:]
#         self.keyword_list = self.keyword_list[23:]
#
#         # 분기 데이터 받아야 하는 리스트 대상으로 한번 반복.
#         # 임시 항목 리스트에 대해 분기 데이터 추출.
#         for index, keyword in self.keyword_list.iterrows():
#             keyword = pd.read_sql("select * from social_keywords where id=" + str(search_item["keyword_id"]), self.db).iloc[0]
#
#             # 누적 데이터 수집 여부 체크.
#             all_accum_mention = self.cur.execute("select * from service_mention_counts "
#                                                  "where keyword_id=" + str(keyword["id"]) + " and term_type=F and term_start=2014-01-01 ")
#             all_accum_connection = self.cur.execute("select * from service_connection_words "
#                                                     "where keyword_id=" + str(keyword["id"]) + " and term_type=F and term_start=2014-01-01 ")
#             all_accum_pos_neg = self.cur.execute("select * from service_pos_neg_words "
#                                                  "where keyword_id=" + str(keyword["id"]) + " and term_type=F and term_start=2014-01-01 ")
#             if (all_accum_mention != None) & (all_accum_connection != None) & (all_accum_pos_neg != None):
#
#             self.start_date = search_item["term_start"]
#             self.start_date_arr = self.start_date.split("-")
#             self.end_date = search_item["term_end"]
#             self.end_date_arr = self.end_date.split("-")
#             search_result = True
#             # 실패시 해당 종목을 3번까지 반복.
#             for try_count in range(3):
#                 if search_result == False:
#                     break;
#
#                 # db에 이미 해당 데이터가 있는지 확인.
#                 #   정확하게 들어 갔는지에 대한 여부가 아닌, 데이터 insert를 이미 한 항목인지 아닌지 여부이다.
#                 try:
#                     self.cur.execute(
#                         "select count(id) from service_mention_counts where keyword_id='" + str(keyword["id"]) + "'" + \
#                         "   and (term_type='D' and term_start <= '" + self.end_date + "' and term_start >= '" + self.start_date + "') "
#                     )
#                     pre_mentions_data = self.cur.fetchone()
#                     #
#                     d1 = datetime.datetime(int(self.start_date_arr[0]), int(self.start_date_arr[1]), int(self.start_date_arr[2]))
#                     d2 = datetime.datetime(int(self.end_date_arr[0]), int(self.end_date_arr[1]), int(self.end_date_arr[2]))
#                     day_count = (d2 - d1).days + 1
#
#                     self.cur.execute(
#                         "select id from service_pos_neg_words where keyword_id='" + str(keyword["id"]) + "'" + \
#                         "   and (term_type='" + self.term_type + "' and term_start='" + self.start_date + "' and term_end='" + self.end_date + "')"
#                     )
#                     pre_pos_neg_data = self.cur.fetchone()
#                     self.cur.execute(
#                         "select id from service_connection_words where keyword_id='" + str(keyword["id"]) + "'" + \
#                         "   and (term_type='" + self.term_type + "' and term_start='" + self.start_date + "' and term_end='" + self.end_date + "')"
#                     )
#                     pre_connection_data = self.cur.fetchone()
#
#                     if (pre_mentions_data[0] == day_count) & (pre_pos_neg_data != None) & (pre_connection_data != None):
#                         break
#                 except Exception as e:
#                     self.report_error(e, keyword["keyword"])
#                     continue
#
#                 try:
#                     # 분석 단어 입력
#                     try:
#                         self.click_element(
#                             "//body/div[contains(@class,'layout-top-bar')]//div[contains(@class,'top-search-input-box')]"
#                             "/div[@id='searchInputArea']/div[contains(@class,'input-keyword')]/button", 2, 1
#                         )
#                     except Exception as e:
#                         self.driver.refresh()
#                         cm.wait(4)
#                         self.click_element(
#                             "//body/div[contains(@class,'layout-top-bar')]//div[contains(@class,'top-search-input-box')]"
#                             "/div[@id='searchInputArea']/div[contains(@class,'input-keyword')]/button", 2
#                         )
#
#                     self.click_element(
#                         "//div[@id='analysisOptionWindowArea']//div[contains(@class,'modal-search-body')]"
#                         "/div[contains(@class,'btn-box')]/button", 2, 1
#                     )
#                     search_keyword = self.driver.find_element_by_xpath(
#                         "//body/div[contains(@class,'layout-top-bar')]//div[contains(@class,'top-search-input-box')]"
#                         "/div[@id='analysisSearchModal']/div[@id='analysisOptionWindowArea']"
#                         "//div[contains(@class,'modal-search-header')]/div/input"
#                     )
#                     search_keyword.send_keys(keyword["keyword"])
#                     # search_keyword.send_keys("NH투자증권")
#                     cm.wait(2)
#
#                     # 동의어, 포함어, 제외어 입력.
#                     # keyword_row = self.keyword_list[self.keyword_list["code_id"]==company["code"]].iloc[0]
#                     equal_keyword_list = keyword["equal_keyword_list"]
#                     # include_keyword_list = keyword_row["or_include_keyword_list"]
#                     include_keyword_list = keyword["and_include_keyword_list"]
#                     exclude_keyword_list = keyword["exclude_keyword_list"]
#
#                     for extra_keyword in equal_keyword_list.split("\\"):
#                         key_input_xpath = \
#                             "//div[@id='analysisOptionWindowArea']//div[contains(@class,'search-detail-condition-block-div')]" \
#                             "//div[contains(@class,'input-item')][1]//input[contains(@class,'synonym-keyword')]"
#                         self.driver.find_element_by_xpath(key_input_xpath).send_keys(extra_keyword)
#                         cm.wait(0, 1)
#                         self.driver.find_element_by_xpath(key_input_xpath).send_keys(Keys.ENTER)
#                         cm.wait(1, 2)
#
#                     for extra_keyword in include_keyword_list.split("\\"):
#                         key_input_xpath = \
#                             "//div[@id='analysisOptionWindowArea']//div[contains(@class,'search-detail-condition-block-div')]" \
#                             "//div[contains(@class,'input-item')][2]//input[contains(@class,'include-keyword')]"
#                         self.driver.find_element_by_xpath(key_input_xpath).send_keys(extra_keyword)
#                         cm.wait(0, 1)
#                         self.driver.find_element_by_xpath(key_input_xpath).send_keys(Keys.ENTER)
#                         cm.wait(1, 2)
#
#                     for extra_keyword in exclude_keyword_list.split("\\"):
#                         key_input_xpath = \
#                             "//div[@id='analysisOptionWindowArea']//div[contains(@class,'search-detail-condition-block-div')]" \
#                             "//div[contains(@class,'input-item')][3]//input[contains(@class,'exclude-keyword')]"
#                         self.driver.find_element_by_xpath(key_input_xpath).send_keys(extra_keyword)
#                         cm.wait(0, 1)
#                         self.driver.find_element_by_xpath(key_input_xpath).send_keys(Keys.ENTER)
#                         cm.wait(1, 2)
#
#                     # 검색 버튼 클릭
#                     self.click_element(
#                         "//body/div[contains(@class,'layout-top-bar')]//div[contains(@class,'top-search-input-box')]"
#                         "/div[@id='analysisSearchModal']/div[contains(@class,'modal-search-footer')]"
#                         "//button[@id='analysisSubmitButton']", 3
#                     )
#
#                     # 기간 입력
#                     self.click_element(
#                         "//body/div[contains(@class,'layout-top-bar')]//div[contains(@class,'top-search-option-box')]"
#                         "//div[@id='inputCalendar']/div[1]/label", 1, 1
#                     )
#                     # 시작일 선택
#                     calender_xpath = "//body/div[contains(@class,'daterangepicker')]/div[contains(@class,'left')]" \
#                                      "//div[contains(@class,'calendar-table')]"
#                     self.click_element(
#                         calender_xpath + "//thead/tr[1]/th[contains(@class,'month')]/select[contains(@class,'yearselect')]",
#                         1
#                     )
#                     self.click_element(
#                         calender_xpath + "//thead/tr[1]/th[contains(@class,'month')]"
#                                          "/select[contains(@class,'yearselect')]/option[contains(@value,'" +
#                         self.start_date.split("-")[0] + "')]", 1
#                     )
#
#                     self.click_element(
#                         calender_xpath + "//thead/tr[1]/th[contains(@class,'month')]"
#                                          "/select[contains(@class,'monthselect')]", 2
#                     )
#                     self.click_element(
#                         calender_xpath + "//thead/tr[1]/th[contains(@class,'month')]"
#                                          "/select[contains(@class,'monthselect')]/option[contains(@value,'" +
#                         str(int(self.start_date.split("-")[1]) - 1) + "')]", 1
#                     )
#
#                     self.click_element(
#                         calender_xpath + "//tbody//td[not(contains(@class,'off')) and contains(text()," +
#                         str(int(self.start_date.split("-")[2])) + ")]", 1
#                     )
#
#                     # 종료일 선택
#                     calender_xpath = "//body/div[contains(@class,'daterangepicker')][2]/div[contains(@class,'left')]" \
#                                      "//div[contains(@class,'calendar-table')]"
#                     self.click_element(
#                         calender_xpath + "//thead/tr[1]/th[contains(@class,'month')]/select[contains(@class,'yearselect')]", 2
#                     )
#                     self.click_element(
#                         calender_xpath + "//thead/tr[1]/th[contains(@class,'month')]"
#                                          "/select[contains(@class,'yearselect')]/option[contains(@value,'" +
#                         self.end_date.split("-")[0] + "')]", 1
#                     )
#
#                     self.click_element(
#                         calender_xpath + "//thead/tr[1]/th[contains(@class,'month')]"
#                                          "/select[contains(@class,'monthselect')]", 1
#                     )
#                     self.click_element(
#                         calender_xpath + "//thead/tr[1]/th[contains(@class,'month')]"
#                                          "/select[contains(@class,'monthselect')]/option[contains(@value,'" +
#                         str(int(self.end_date.split("-")[1]) - 1) + "')]", 1
#                     )
#
#                     self.click_element(
#                         calender_xpath + "//tbody//td[not(contains(@class,'off')) and contains(text()," +
#                         str(int(self.end_date.split("-")[2])) + ")]", 1
#                     )
#
#                     # 소셜 종류 선택
#                     social_list_count = self.driver.find_elements_by_xpath(
#                         "//body/div[contains(@class,'layout-top-bar')]//div[contains(@class,'top-search-option-box')]"
#                         "//ul[@id='channelCheckBoxUl']/li"
#                     )
#                     for social_type_index in range(1, len(social_list_count) + 1):
#                         social_type = self.driver.find_element_by_xpath(
#                             "//body/div[contains(@class,'layout-top-bar')]//div[contains(@class,'top-search-option-box')]"
#                             "//ul[@id='channelCheckBoxUl']/li[" + str(social_type_index) + "]//input[@type='checkbox']"
#                         )
#                         if social_type.is_selected() == False:
#                             # social_type.click()
#                             self.driver.execute_script("arguments[0].click();", social_type)
#                             cm.wait(1)
#                     temp_button = self.driver.find_element_by_xpath(
#                         "//body/div[contains(@class,'layout-top-bar')]//div[contains(@class,'top-search-option-box')]"
#                         "//button[@id='searchConditionApplyButton']"
#                     )
#                     if temp_button.get_attribute("disabled") != "true":
#                         self.click_element(
#                             "//body/div[contains(@class,'layout-top-bar')]//div[contains(@class,'top-search-option-box')]"
#                             "//button[@id='searchConditionApplyButton']", 2
#                         )
#
#                     # 언급량 메뉴 선택
#                     self.click_element(
#                         "//body/div[contains(@class,'layout-left')]//div[contains(@class,'lnb-wrap-banner')]/nav/ul/li[1]"
#                         "/ul//a[contains(text(),'언급량 분석')]", 2
#                     )
#                     try:
#                         WebDriverWait(self.driver, 3).until(
#                             EC.visibility_of_element_located((By.XPATH, "//div[@id='driver-popover-item']"
#                                                                         "/div[contains(@class,'driver-popover-footer')]/div[contains(@class,'vguide-pop-session')]"))
#                         )
#                         cm.wait(1)
#                         self.driver.find_element_by_xpath("//div[@id='driver-popover-item']"
#                                                           "/div[contains(@class,'driver-popover-footer')]/div[contains(@class,'vguide-pop-session')]"
#                                                           ).click()
#                         cm.wait(2)
#                     except Exception as e:
#                         pass
#
#                     # 데이터 없는 경우 처리
#                     cm.wait(2)
#                     no_data_section = self.driver.find_element_by_xpath("//section[@id='noDataArea']")
#                     if "display" not in no_data_section.get_attribute("style"):
#                         self.report_error(keyword=keyword["keyword"], msg="데이터 없음")
#                         break
#
#                     # 언급량 엑셀 다운.
#                     WebDriverWait(self.driver, 60).until(
#                         EC.visibility_of_element_located((By.XPATH, "//input[@id='D-sensibility']/following-sibling::label"))
#                     )
#                     cm.wait(3)
#                     temp_button = self.driver.find_element_by_xpath("//input[@id='D-sensibility']")
#                     if temp_button.is_selected() == False:
#                         self.click_element("//input[@id='D-sensibility']", 2)
#                     WebDriverWait(self.driver, 60).until(
#                         EC.visibility_of_element_located((By.XPATH, "//button[@id='mentionExcelDownloadButton']"))
#                     )
#                     cm.wait(3)
#                     # 기존 동일한 파일 있을시, 삭제.
#                     mentions_count_file_path = constant.download_path + "/sometrend/[썸트렌드] " + keyword["keyword"] + "_언급량_" + \
#                                                self.start_date[2:].replace("-", "") + "-" + self.end_date[2:].replace("-", "") + ".xlsx"
#                     if os.path.isfile(mentions_count_file_path):
#                         os.remove(mentions_count_file_path)
#                     self.click_element("//button[@id='mentionExcelDownloadButton']", 4)
#                     if self.wait_file_download(mentions_count_file_path, 30) == False:
#                         raise Exception("파일 다운 시간 초과")
#
#                     # 연관어 분석 메뉴 선택
#                     self.click_element(
#                         "//body/div[contains(@class,'layout-left')]//div[contains(@class,'lnb-wrap-banner')]/nav/ul/li[1]"
#                         "/ul//a[contains(text(),'연관어 분석')]", 2
#                     )
#                     try:
#                         WebDriverWait(self.driver, 3).until(
#                             EC.visibility_of_element_located((By.XPATH, "//div[@id='driver-popover-item']"
#                                                                         "/div[contains(@class,'driver-popover-footer')]/div[contains(@class,'vguide-pop-session')]"))
#                         )
#                         cm.wait(1)
#                         self.driver.find_element_by_xpath(
#                             "//div[@id='driver-popover-item']"
#                             "/div[contains(@class,'driver-popover-footer')]/div[contains(@class,'vguide-pop-session')]"
#                         ).click()
#                         cm.wait(2)
#                     except Exception as e:
#                         pass
#
#                     # 데이터 없는 경우 처리
#                     cm.wait(2)
#                     no_data_section = self.driver.find_element_by_xpath("//section[@id='noDataArea']")
#                     if "display" not in no_data_section.get_attribute("style"):
#                         self.report_error(keyword=keyword["keyword"], msg="데이터 없음")
#                         break
#
#                     # 연관어 엑셀 다운
#                     WebDriverWait(self.driver, 60).until(
#                         EC.visibility_of_element_located((By.XPATH, "//input[@id='W-sensibility']/following-sibling::label"))
#                     )
#                     cm.wait(3)
#                     temp_button = self.driver.find_element_by_xpath("//input[@id='W-sensibility']")
#                     if temp_button.is_selected() == False:
#                         self.click_element("//input[@id='W-sensibility']", 2)
#                     WebDriverWait(self.driver, 60).until(
#                         EC.visibility_of_element_located((By.XPATH,
#                                                           "//div[@id='rankingChangeListForAssociationVskeleton']/div[contains(@class,'layout-card-header')]"
#                                                           "//div[contains(@class,'layout-card-header-buttons')]/button[contains(@class,'btn-excel-down')]"))
#                     )
#                     cm.wait(3)
#                     # 기존 파일 있을시 삭제.
#                     connection_word_file_path = constant.download_path + "/sometrend/[썸트렌드] " + keyword["keyword"] + "_연관어 순위 변화_" + \
#                                                 self.start_date[2:].replace("-", "") + "-" + self.end_date[2:].replace("-", "") + ".xlsx"
#                     if os.path.isfile(connection_word_file_path):
#                         os.remove(connection_word_file_path)
#                     self.click_element(
#                         "//div[@id='rankingChangeListForAssociationVskeleton']/div[contains(@class,'layout-card-header')]"
#                         "//div[contains(@class,'layout-card-header-buttons')]/button[contains(@class,'btn-excel-down')]", 4
#                     )
#                     if self.wait_file_download(connection_word_file_path, 30) == False:
#                         raise Exception("파일 다운 시간 초과.")
#
#                     # 긍,부정어 분석 메뉴 선택
#                     self.click_element(
#                         "//body/div[contains(@class,'layout-left')]//div[contains(@class,'lnb-wrap-banner')]/nav/ul/li[1]"
#                         "/ul//a[contains(text(),'긍 · 부정 분석')]", 2
#                     )
#                     try:
#                         WebDriverWait(self.driver, 3).until(
#                             EC.visibility_of_element_located((By.XPATH, "//div[@id='driver-popover-item']"
#                                                                         "/div[contains(@class,'driver-popover-footer')]/div[contains(@class,'vguide-pop-session')]"))
#                         )
#                         cm.wait(1)
#                         self.driver.find_element_by_xpath(
#                             "//div[@id='driver-popover-item']"
#                             "/div[contains(@class,'driver-popover-footer')]/div[contains(@class,'vguide-pop-session')]"
#                         ).click()
#                         cm.wait(2)
#                     except Exception as e:
#                         pass
#
#                     # 데이터 없는 경우 처리
#                     cm.wait(2)
#                     no_data_section = self.driver.find_element_by_xpath("//section[@id='noDataArea']")
#                     if "display" not in no_data_section.get_attribute("style"):
#                         self.report_error(keyword=keyword["keyword"], msg="데이터 없음")
#                         break
#
#                     # 긍,부정어 엑셀 다운
#                     WebDriverWait(self.driver, 60).until(
#                         EC.visibility_of_element_located((By.XPATH, "//input[@id='W-sensibility01']/following-sibling::label"))
#                     )
#                     cm.wait(3)
#                     temp_button = self.driver.find_element_by_xpath("//input[@id='W-sensibility01']")
#                     if temp_button.is_selected() == False:
#                         self.click_element("//input[@id='W-sensibility01']", 2)
#                     WebDriverWait(self.driver, 60).until(
#                         EC.visibility_of_element_located((By.XPATH,
#                                                           "//div[@id='changeRankListVskeleton']/div[contains(@class,'layout-card-header')]"
#                                                           "//div[contains(@class,'layout-card-header-buttons')]/button[contains(@class,'btn-excel-down')]"
#                                                           ))
#                     )
#                     cm.wait(3)
#                     # 기존 파일 있을시 삭제.
#                     pos_neg_word_file_path = constant.download_path + "/sometrend/[썸트렌드] " + keyword["keyword"] + "_긍부정 단어 순위 변화_" + \
#                                              self.start_date[2:].replace("-", "") + "-" + self.end_date[2:].replace("-", "") + ".xlsx"
#                     if os.path.isfile(pos_neg_word_file_path):
#                         os.remove(pos_neg_word_file_path)
#                     self.click_element(
#                         "//div[@id='changeRankListVskeleton']/div[contains(@class,'layout-card-header')]"
#                         "//div[contains(@class,'layout-card-header-buttons')]/button[contains(@class,'btn-excel-down')]",
#                         4
#                     )
#                     if self.wait_file_download(pos_neg_word_file_path, 30) == False:
#                         raise Exception("파일 다운 시간 초과")
#
#                     # db 저장
#                     self.store_data(keyword)
#
#                     # self.cur.execute("update into social_search_list where id="+search_item["keyword_id"])
#
#                     # 스크래핑 수행한 종목 개수 카운트.
#                     self.search_count = self.search_count + 1
#                     # 목표 스크래핑 개수를 마치면 종료.
#                     if self.search_count % 30 == 0:
#                         self.driver.quit()
#                         self.initial_setting()
#
#                     cm.wait(30, search_count=self.search_count, search_count_max=self.scraping_count_goal)
#
#                     break
#
#                 except NoSuchWindowException as e:
#                     # 에러 정보 저장.
#                     self.report_error(e, keyword["keyword"])
#
#                     self.driver.quit()
#                     cm.wait(4)
#                     self.initial_setting()
#
#                     continue
#
#                 except Exception as e:
#                     self.report_error(e, keyword["keyword"])
#
#                     cm.wait(4)
#                     continue
#
#     def store_data(self, keyword, ):
#
#         # 엑셀 데이터를 dataframe으로 가공
#         mention_count_df = pd.read_excel(constant.download_path + "/sometrend/[썸트렌드] " + keyword["keyword"] + "_언급량_" +
#                                          self.start_date[2:].replace("-", "") + "-" + self.end_date[2:].replace("-", "") + ".xlsx")
#         pos_neg_words_df = pd.read_excel(constant.download_path + "/sometrend/[썸트렌드] " + keyword["keyword"] + "_긍부정 단어 순위 변화_" +
#                                          self.start_date[2:].replace("-", "") + "-" + self.end_date[2:].replace("-", "") + ".xlsx")
#         connection_words_df = pd.read_excel(constant.download_path + "/sometrend/[썸트렌드] " + keyword["keyword"] + "_연관어 순위 변화_" +
#                                             self.start_date[2:].replace("-", "") + "-" + self.end_date[2:].replace("-", "") + ".xlsx")
#
#         first_index = mention_count_df[mention_count_df[mention_count_df.columns[0]] == "날짜"].index
#         mention_count_df.columns = mention_count_df.loc[first_index[0]]
#         mention_count_df = mention_count_df.loc[first_index[0] + 1:]
#
#         first_index = pos_neg_words_df[pos_neg_words_df[pos_neg_words_df.columns[0]] == "순위"].index - 1
#         pos_neg_words_df.columns = pos_neg_words_df.loc[first_index[0]]
#         pos_neg_words_df = pos_neg_words_df.loc[first_index[0] + 1:]
#
#         first_index = connection_words_df[connection_words_df[connection_words_df.columns[0]] == "순위"].index - 1
#         connection_words_df.columns = connection_words_df.loc[first_index[0]]
#         connection_words_df = connection_words_df.loc[first_index[0] + 1:]
#
#         # 언급량 데이터에 대해 해당 종목, 기간 단위, 기간에 대해 기존 데이터가 있을 경우 삭제
#         self.cur.execute(
#             "delete from service_mention_counts where keyword_id='" + str(keyword["id"]) + "'" + \
#             "   and ( (term_type='D' and term_start <= '" + self.end_date + "' and term_start >= '" + self.start_date + "') " + \
#             "           )"
#         )
#         self.db.commit()
#
#         # 언급량 데이터 저장
#         insert_sql = ""
#         for index, row in mention_count_df.iterrows():
#             insert_sql = insert_sql + ", " + \
#                          "( '" + str(datetime.datetime.now(datetime.timezone.utc)) + "', " + \
#                          "'" + str(datetime.datetime.now(datetime.timezone.utc)) + "', " + \
#                          "'D', '" + row["날짜"].replace(".", "-") + "', '" + row["날짜"].replace(".", "-") + "'," + \
#                          str(row["커뮤니티"]) + ", " + str(row["인스타그램"]) + ", " + \
#                          "" + str(row["블로그"]) + ", " + str(row["뉴스"]) + ", " + str(row["트위터"]) + ", " + str(row["합계"]) + ", '" + str(keyword["id"]) + "' )"
#
#         insert_sql = insert_sql[1:]
#         try:
#             self.cur.execute(
#                 "insert into service_mention_counts "
#                 "   ( created_at, updated_at, term_type, term_start, term_end, community_count, insta_count, "
#                 "       blog_count, news_count, twitter_count, count_sum, keyword_id) "
#                 "values " + insert_sql
#             )
#             self.db.commit()
#         except Exception as e:
#             self.db.rollback()
#             self.report_error(e, keyword["keyword"], msg="db insert 에러")
#
#         # 긍, 부정 데이터 기존 데이터 삭제.
#         self.cur.execute(
#             "delete from service_pos_neg_words where keyword_id='" + str(keyword["id"]) + "'" + \
#             "   and ( (term_type='W' and term_end <= '" + self.end_date + "' and term_start >= '" + self.start_date + "') " + \
#             "           or (term_type='" + self.term_type + "' and term_start='" + self.start_date + "' and term_end='" + self.end_date + "'))"
#         )
#         self.db.commit()
#
#         # 긍 부정 데이터 저장.
#         columns_index_list = []
#         for i in range(pos_neg_words_df.columns.size):
#             if re.search(r"\d{4}.\d{2}.\d{2}~\d{4}.\d{2}.\d{2}", str(pos_neg_words_df.columns[i])) != None:
#                 columns_index_list.append(i)
#
#         # 완전한 주가 아닌 경우 패스. 첫 주와 끝 주가 완전한 주가 아니면 패스. 그리고 끝 주는 엑셀 컬럼상에는 완전한 주로 되어있지만 실제 데이터는 끝 기간까지만 카운팅하고 있다.
#         # 이 부분도 패스시켜야 한다. 안그럼 insert를 엑셀 기간 컬럼 값을 기준으로 하기 때문에, delete시 캐치되지 않는 기간이 insert되어 unique오류 발생.
#         if datetime.date.fromisoformat(self.start_date).weekday() != 0:
#             columns_index_list.pop(0)
#         if datetime.date.fromisoformat(self.end_date).weekday() != 6:
#             columns_index_list.pop(-2)
#
#         # 스크래핑 대상 기간이 주 단위일 경우, 엑셀상에 같은 주 대상 데이터가 두 개 나오므로 하나 날림.
#         if self.term_type == "W":
#             columns_index_list = columns_index_list[1:]
#
#         insert_sql = ""
#         for column_index in columns_index_list:
#             # 한 주에 대한 dataframe 가공.
#             week_list = pos_neg_words_df.take([column_index, column_index + 1, column_index + 2, column_index + 3], axis=1)
#             week_list = week_list.reset_index(drop=True)
#             week_list.columns = week_list.iloc[0]
#             week_list = week_list.drop([0])
#             week_list = week_list.dropna()
#
#             last_term = self.start_date.split("-")[0] + "." + self.start_date.split("-")[1] + "." + \
#                         self.start_date.split("-")[2] + "~" + \
#                         self.end_date.split("-")[0] + "." + self.end_date.split("-")[1] + "." + \
#                         self.end_date.split("-")[2]
#
#             term_type = "W"
#             term_start = pos_neg_words_df.columns[column_index].split("~")[0].replace(".", "-")
#             term_end = pos_neg_words_df.columns[column_index].split("~")[1].replace(".", "-")
#
#             if pos_neg_words_df.columns[column_index] == last_term:
#                 term_type = self.term_type
#
#             week_list = week_list.groupby(week_list["긍부정 단어"]).sum().sort_values(by="건수", ascending=False)
#
#             # 데이터 insert sql 작성.
#             for word_i in range(len(week_list)):
#                 pos_neg = ""
#                 if week_list.iloc[word_i]["긍부정 구분"] == "부정":
#                     pos_neg = "NEG"
#                 elif week_list.iloc[word_i]["긍부정 구분"] == "긍정":
#                     pos_neg = "POS"
#                 elif week_list.iloc[word_i]["긍부정 구분"] == "중립":
#                     pos_neg = "NEU"
#
#                 insert_sql = insert_sql + ", " + \
#                              "( '" + str(datetime.datetime.now(datetime.timezone.utc)) + "', " + \
#                              "'" + str(datetime.datetime.now(datetime.timezone.utc)) + "', " + \
#                              "'" + term_type + "', '" + term_start + "', '" + term_end + "', " + \
#                              "'" + week_list.index[word_i] + "', " + str(week_list.iloc[word_i]["건수"]) + ", " + \
#                              "" + str(word_i + 1) + ", '" + pos_neg + "', '" + week_list.iloc[word_i]["속성"] + "', '" + str(keyword["id"]) + "')"
#
#         insert_sql = insert_sql[1:]
#         try:
#             self.cur.execute(
#                 "insert into service_pos_neg_words"
#                 "       (created_at, updated_at, term_type, term_start, term_end, word, word_count, "
#                 "           rank, pos_neg, property, keyword_id) "
#                 "values " + insert_sql
#             )
#             self.db.commit()
#         except Exception as e:
#             self.db.rollback()
#             self.report_error(e, keyword["keyword"], msg="db insert 에러")
#
#         # 연관어 기존 데이터 삭제
#         self.cur.execute(
#             "delete from service_connection_words where keyword_id='" + str(keyword["id"]) + "'" + \
#             "   and ( (term_type='W' and term_end <= '" + self.end_date + "' and term_start >= '" + self.start_date + "') " + \
#             "           or (term_type='" + self.term_type + "' and term_start='" + self.start_date + "' and term_end='" + self.end_date + "'))"
#         )
#         self.db.commit()
#
#         # 연관어 저장.
#         columns_index_list = []
#         for i in range(connection_words_df.columns.size):
#             if re.search(r"\d{4}.\d{2}.\d{2}~\d{4}.\d{2}.\d{2}", str(connection_words_df.columns[i])) != None:
#                 columns_index_list.append(i)
#
#         # 긍부정어와 마찬가지로 첫 부분, 끝 부분 처리 필요.
#         if datetime.date.fromisoformat(self.start_date).weekday() != 0:
#             columns_index_list.pop(0)
#         if datetime.date.fromisoformat(self.end_date).weekday() != 6:
#             columns_index_list.pop(-2)
#
#         # 스크래핑 대상 기간이 주 단위일 경우, 엑셀상에 같은 주 대상 데이터가 두 개 나오므로 하나 날림.
#         if self.term_type == "W":
#             columns_index_list = columns_index_list[1:]
#
#         insert_sql = ""
#         for column_index in columns_index_list:
#             # 한 주에 대한 dataframe 가공.
#             week_list = connection_words_df.take([column_index, column_index + 1, column_index + 2, column_index + 3],
#                                                  axis=1)
#             week_list = week_list.reset_index(drop=True)
#             week_list.columns = week_list.iloc[0]
#             week_list = week_list.drop([0])
#             week_list = week_list.dropna()
#
#             last_term = self.start_date.split("-")[0] + "." + self.start_date.split("-")[1] + "." + \
#                         self.start_date.split("-")[2] + "~" + \
#                         self.end_date.split("-")[0] + "." + self.end_date.split("-")[1] + "." + \
#                         self.end_date.split("-")[2]
#
#             term_type = "W"
#             term_start = connection_words_df.columns[column_index].split("~")[0].replace(".", "-")
#             term_end = connection_words_df.columns[column_index].split("~")[1].replace(".", "-")
#
#             if connection_words_df.columns[column_index] == last_term:
#                 term_type = self.term_type
#
#             week_list = week_list.groupby(week_list["연관어"]).sum().sort_values(by="건수", ascending=False)
#
#             # 데이터 insert sql 작성.
#             for word_i in range(len(week_list)):
#                 insert_sql = insert_sql + ", " + \
#                              "( '" + str(datetime.datetime.now(datetime.timezone.utc)) + "', " + \
#                              "'" + str(datetime.datetime.now(datetime.timezone.utc)) + "', " + \
#                              "'" + term_type + "', '" + term_start + "', '" + term_end + "', " + \
#                              "'" + week_list.index[word_i] + "', " + str(week_list.iloc[word_i]["건수"]) + ", " + \
#                              "" + str(word_i + 1) + ", '" + week_list.iloc[word_i]["카테고리 대분류"] + "', '" + week_list.iloc[word_i]["카테고리 소분류"] + \
#                              "', '" + str(keyword["id"]) + "')"
#
#         insert_sql = insert_sql[1:]
#         try:
#             self.cur.execute(
#                 "insert into service_connection_words"
#                 "       (created_at, updated_at, term_type, term_start, term_end, word, word_count, "
#                 "           rank, category1, category2, keyword_id) "
#                 "values " + insert_sql
#             )
#             self.db.commit()
#         except Exception as e:
#             self.db.rollback()
#             self.report_error(e, keyword["keyword"], msg="db insert 에러")
#
#     def move_file(self, pre_path, path):
#         if os.path.isfile(path):
#             os.remove(path)
#         os.rename(pre_path, path)
#
#     def wait_file_download(self, path, wait_time):
#         # 다운 완료 체크
#         for i in range(wait_time):
#             if os.path.isfile(path):
#                 return True
#             else:
#                 time.sleep(1)
#         else:
#             return False
#
#     def report_error(self, e=None, keyword="", msg=""):
#         date_time = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time()))
#         with open(constant.error_file_path + "/social_search_error_list_" +
#                   time.strftime("%Y-%m-%d", time.localtime(time.time())) + ".txt", "a", encoding="UTF-8") as f:
#             f.write(date_time + "_" + keyword + "_" + msg + "\n")
#             f.write(traceback.format_exc())
#
#     def check_element(self, xpath, condition=EC.visibility_of_element_located, wait_time=5):
#         try:
#             WebDriverWait(self.driver, wait_time).until(condition((By.XPATH, xpath)))
#             cm.wait(3)
#             return True
#         except Exception as e:
#             return False
#
#     def click_element(self, xpath, wait_time, term=None):
#         button = self.driver.find_element_by_xpath(xpath)
#         try:
#             button.click()
#         except ElementNotInteractableException as e:
#             self.driver.execute_script("arguments[0].click();", button)
#         # except Exception as e:
#         #     self.driver.execute_script("arguments[0].click();", button)
#         if term == None:
#             cm.wait(wait_time)
#         else:
#             cm.wait(wait_time, term)
#
#     def wait(self, wait_time, term=5, search_count=None):
#         now = datetime.datetime.now().time()
#         # 검색 쿼리 횟수 제한
#         if (search_count != None):
#             if (search_count >= self.scraping_count_goal):
#                 print(str(datetime.datetime.now()) + "search count limit break term start.")
#                 # self.report_error(msg="search count limit break term start.")
#                 while (datetime.datetime.now().time() < datetime.time(6, 0)) | \
#                         (datetime.datetime.now().time() > datetime.time(7, 0)):
#                     time.sleep(10)
#                 else:
#                     print(str(datetime.datetime.now()) + "search count limit break term end.")
#                     # self.report_error(msg="search count limit break term end.")
#                     self.start_time = datetime.time(int(random.triangular(9, 10, 9)),
#                                                     int(random.randrange(0, 59, 1)))
#                     self.break_time = datetime.time(int(random.triangular(12, 13, 13)),
#                                                     int(random.randrange(0, 59, 1)))
#                     self.end_time = datetime.time(int(random.triangular(17, 19, 18)),
#                                                   int(random.randrange(0, 59, 1)))
#                     self.search_count = 0
#
#         # 시작시간, 중간 쉬는 시간, 끝시간에 따른 대기.
#         if (self.start_time >= now) | (self.end_time <= now):
#             if self.start_time >= now:
#                 print(str(datetime.datetime.now()) + "start break term start.")
#             elif self.end_time <= now:
#                 print(str(datetime.datetime.now()) + "end break term start.")
#             # self.report_error(msg="start break term start.")
#             while (self.start_time >= datetime.datetime.now().time()) | \
#                     (self.end_time <= datetime.datetime.now().time()):
#                 time.sleep(10)
#             else:
#                 print(str(datetime.datetime.now()) + "start/end break term end.")
#                 # self.report_error(msg="start break term end.")
#                 self.search_count = 0
#                 self.start_time = datetime.time(int(random.triangular(9, 10, 9)),
#                                                 int(random.randrange(0, 59, 1)))
#                 self.end_time = datetime.time(int(random.triangular(17, 19, 19)),
#                                               int(random.randrange(0, 59, 1)))
#                 self.break_time = datetime.time(int(random.triangular(12, 13, 13)),
#                                                 int(random.randrange(0, 30, 1)))
#                 print((str(datetime.datetime.now()) + "break_time : " + str(self.break_time)))
#                 print((str(datetime.datetime.now()) + "start_time : " + str(self.start_time)))
#                 print((str(datetime.datetime.now()) + "end_time : " + str(self.end_time)))
#
#         elif (self.break_time < now) & \
#                 ((datetime.datetime.combine(datetime.date.today(), self.break_time)
#                   + datetime.timedelta(minutes=30)).time() > now):
#             print(str(datetime.datetime.now()) + "middle break term start.")
#             # self.report_error(msg="middle break term start.")
#             time.sleep(random.normalvariate(3000, 400))
#             print(str(datetime.datetime.now()) + "middle break term end.")
#             # self.report_error(msg="middle break term end.")
#
#         # 랜덤 몇 초 더 대기.
#         random_value = random.randrange(1, 100, 1)
#         if random_value % 20 == 0:
#             print(str(datetime.datetime.now()) + "more sleep...")
#             time.sleep(random.triangular(wait_time, wait_time + term + 10, wait_time + term + 5))
#         # print(str(datetime.datetime.now()) + "...")
#         time.sleep(random.triangular(wait_time, wait_time + term, wait_time))
#         # 랜덤 3~5분 대기.
#         random_value3 = random.randrange(1, 100, 1)
#         if random_value3 % 100 == 0:
#             print(str(datetime.datetime.now()) + "3~5minute sleep")
#             # self.report_error(msg="3~5minute sleep")
#             time.sleep(random.uniform(180, 300))
#         # 랜덤 10~20분 대기.
#         random_value2 = random.randrange(1, 1000, 1)
#         if random_value2 % 500 == 0:
#             print(str(datetime.datetime.now()) + "10~20minute sleep")
#             # self.report_error(msg="10~20minute sleep")
#             time.sleep(random.uniform(600, 1200))
#
#     def initial_setting(self):
#         for i in range(10):
#             try:
#                 # driver 실행.
#                 # self.driver.quit()
#                 chrome_driver = constant.chrome_driver_path
#                 chrome_options = Options()
#                 chrome_options.add_experimental_option("prefs", {
#                     "download.default_directory": constant.download_path.replace("/", "\\") + "\\sometrend",
#                     "profile.content_settings.exceptions.automatic_downloads.*.setting": 1,
#                     "plugins.always_open_pdf_externally": True
#                 })
#                 self.driver = webdriver.Chrome(chrome_driver, chrome_options=chrome_options)
#                 self.driver.set_window_position(900, 0)
#                 self.driver.set_window_size(1600, 1400)
#                 self.driver.get('https://some.co.kr/')
#                 self.driver.implicitly_wait(5)
#                 cm.wait(2)
#
#                 # 검색 페이지 세팅.
#                 #   초기 화면 팝업창 제거
#                 popup = self.driver.find_element_by_xpath(
#                     "//footer/div[contains(@class,'popup_banner_container')]"
#                 )
#                 if "none" not in popup.get_attribute("style"):
#                     self.click_element("//footer/div[contains(@class,'popup_banner_container')]/aside/ul/li[1]/a", 2)
#
#                 #   로그인
#                 temp_button = self.driver.find_element_by_xpath(
#                     "//div[contains(@class,'main_wrap')]//header/nav//article[2]//a[contains(@class,'btn-login')]"
#                 )
#                 self.driver.execute_script("arguments[0].click();", temp_button)
#                 cm.wait(2)
#
#                 self.driver.find_element_by_xpath(
#                     "//div[@id='wrap']/section/div[contains(@class,'login_form_box')]/form[@id='loginForm']"
#                     "//input[@id='username']"
#                 ).send_keys("jonghee5347@gmail.com")
#                 cm.wait(2)
#                 self.driver.find_element_by_xpath(
#                     "//div[@id='wrap']/section/div[contains(@class,'login_form_box')]/form[@id='loginForm']"
#                     "//input[@id='password']"
#                 ).send_keys(")!kaimobile01")
#                 cm.wait(2)
#                 temp_button = self.driver.find_element_by_xpath(
#                     "//div[@id='wrap']/section/div[contains(@class,'login_form_box')]/form[contains(@class,'login_frm')]"
#                     "/div[contains(@class,'login_btn_box')]/a[contains(@class,'login_btn')]"
#                 )
#                 self.driver.execute_script("arguments[0].click();", temp_button)
#                 cm.wait(2)
#
#                 # 중복 로그인 체크
#                 if self.check_element("//aside[@id='concurrentLogin_wrap']") == True:
#                     self.click_element(
#                         "//aside[@id='concurrentLogin_wrap']//div[contains(@class,'confirm_button_box')]"
#                         "/a[contains(text(),'확인')]", 3
#                     )
#
#                 # 소셜 분석 센터 이동
#                 temp_button = self.driver.find_element_by_xpath(
#                     "//div[contains(@class,'main_wrap')]//header/nav//article[1]//li[contains(@class,'gnb-menu')]"
#                     "/a[contains(text(),'소셜 분석 센터')]"
#                 )
#                 self.driver.execute_script("arguments[0].click();", temp_button)
#                 cm.wait(2)
#
#                 # 설명 팝업 확인 및 처리
#                 try:
#                     WebDriverWait(self.driver, 5).until(
#                         EC.visibility_of_element_located((By.XPATH, "//div[@id='driver-popover-item']"
#                                                                     "/div[contains(@class,'driver-popover-footer')]/div[contains(@class,'vguide-pop-session')]"))
#                     )
#                     cm.wait(1, 2)
#                     self.driver.find_element_by_xpath(
#                         "//div[@id='driver-popover-item']"
#                         "/div[contains(@class,'driver-popover-footer')]/div[contains(@class,'vguide-pop-session')]"
#                     ).click()
#                     cm.wait(2)
#                 except Exception as e:
#                     pass
#
#             except Exception as e:
#                 date_time = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time()))
#                 with open(constant.error_file_path + "/social_keyword_error_list_" + time.strftime("%Y-%m-%d", time.localtime(
#                         time.time())) + ".txt",
#                           "a", encoding="UTF-8") as f:
#                     f.write(date_time + "_초기 세팅 실패.\n")
#                     f.write(traceback.format_exc())
#                 self.driver.quit()
#                 cm.wait(5 + i)
#                 continue
#             else:
#                 break
#
#

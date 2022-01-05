# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
import multiprocessing
import os.path
import random
import subprocess
import sys
import traceback
from datetime import datetime, timezone
from multiprocessing import freeze_support

import scrapy.crawler
from selenium import webdriver
from selenium.webdriver import Proxy
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.proxy import ProxyType


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')
    freeze_support()

    print("setting scheduler")

    import pandas as pd
    # import matplotlib.pyplot as plt
    # from bs4 import BeautifulSoup
    # import requests
    import time
    from scrapy import cmdline
    import psycopg2
    import numpy as np
    import re
    import schedule_job

    # from dart_and_daishin import getStackedHistoricalData
    # import calculate_price_predict

    # cmdline.execute("scrapy crawl naver_finance".split())
    # cmdline.execute("scrapy crawl korean_daily_finance_spider -a quarter=2021-09-30".split())
    # cmdline.execute("scrapy crawl noname".split())
    # cmdline.execute("scrapy crawl report_spider".split())
    # cmdline.execute("scrapy crawl notice_spider".split())
    # cmdline.execute("scrapy crawl sector_spider".split())
    # cmdline.execute("scrapy crawl theme_spider -a target_term=2020-12-31 -a pre_target_term=2019-12-31".split())
    # cmdline.execute("scrapy crawl social_keyword_spider -a start_date=2021-04-01 -a end_date=2021-06-30 -a term_type=Q "
    #                 "-a scraping_count_goal=50".split())
    # cmdline.execute("scrapy crawl big_kinds_news_spider -a start_date=1990-01-01 -a end_date=2021-08-08".split())
    # cmdline.execute("scrapy crawl new_stock_financial_spider".split())
    # cmdline.execute("scrapy crawl fnguide_report_summary_spider -a start_date=2021-01-01 -a end_date=2021-12-24".split())
    # cmdline.execute("scrapy crawl naver_news_spider -a start_date=2020-07-01 -a end_date=2020-07-02".split())
    # cmdline.execute("scrapy crawl deepsearch_stock_info_spider -a target_date=2021-09-30 -a scraping_count_goal=200".split())
    # cmdline.execute("scrapy crawl social_search_spider -a is_followed=True -a start_date=2021-01-01 -a end_date=2021-11-28 -a scraping_count_goal=100 -a follow_start_year=2021".split())
    # cmdline.execute("scrapy crawl daum_news_spider -a start_date=2020-07-01 -a end_date=2020-07-02".split())
    cmdline.execute("scrapy crawl bbd_custom_keyword_naver_news_spider".split())
    # cmdline.execute("scrapy crawl bbd_custom_keyword_daum_news_spider".split())

    # 분기 데이터 스크래핑 스케줄러.
    from apscheduler.schedulers.background import BackgroundScheduler, BlockingScheduler

    scheduler = BlockingScheduler()

    def create_sector_check_process():
        print("create_sector_check_process")
        process = multiprocessing.Process(target=schedule_job.sector_scraping_check)
        process.start()
        process.join()
    def create_theme_check_process():
        print("create_sector_check_process")
        process = multiprocessing.Process(target=schedule_job.sector_scraping_check)
        process.start()
        process.join()
    def create_daily_check_process():
        print("create_daily_check_process")
        process = multiprocessing.Process(target=schedule_job.daily_check)
        process.start()
        process.join()
    def create_social_keyword_scraping_process():
        print("create_social_keyword_scraping_process")
        process = multiprocessing.Process(target=schedule_job.social_keyword_scraping)
        process.start()
        process.join()
    def create_big_kinds_news_scraping_process():
        print("create_big_kinds_news_scraping_process")
        process = multiprocessing.Process(target=schedule_job.big_kinds_news_scraping)
        process.start()
        process.join()
    def create_test_process():
        print("create_test_process")
        process = multiprocessing.Process(target=schedule_job.test)
        process.start()
        process.join()

    # subprocess.Popen("scrapy crawl report_spider".split(), shell=True)
    # scheduler.add_job(create_sector_check_process, 'cron', hour=17, minute=7)
    # scheduler.add_job(create_theme_check_process, 'cron', hour=17, minute=7)
    # scheduler.add_job(create_daily_check_process, 'cron', hour=17, minute=21)
    # scheduler.add_job(create_social_keyword_scraping_process, 'cron', hour=4, minute=0)
    # scheduler.add_job(create_big_kinds_news_scraping_process(), 'cron', hour=1, minute=0)
    # scheduler.add_job()
    # scheduler.start()

    def social_scraping_data_to_excel():
        db = psycopg2.connect(host="112.220.72.179", dbname="openmetric", user="openmetric",
                              password=")!metricAdmin01", port=2345)
        cur = db.cursor()

        pattern_group_list = pd.read_sql(  # 키워드 패턴 목록 추출.
            "select and_include_keyword_list from social_keywords "
            "where is_followed=false and is_deleted=false "#and created_at > '2021-09-01' and created_at < '2021-09-30' "
            "group by and_include_keyword_list "
            "", db)
        # 키워드 패턴 목록에 대해 반복.
        for index, pattern_group in pattern_group_list.iterrows():
            pattern_group = pattern_group["and_include_keyword_list"]

            brand_mention_df = pd.DataFrame(columns=["키워드", "합계", "커뮤니티", "인스타","블로그","뉴스","트위터"])
            company_mention_df = pd.DataFrame(columns=["키워드", "합계", "커뮤니티", "인스타","블로그","뉴스","트위터"])
            brand_pos_neg_df = pd.DataFrame(columns=["키워드", "긍정", "부정", "중립", "공감"])
            company_pos_neg_df = pd.DataFrame(columns=["키워드", "긍정", "부정", "중립", "공감"])

            keyword_list = pd.read_sql(  # 같은 패턴에 해당하는 키워드 목록.
                "select * from social_keywords "
                "where is_followed=false and is_deleted=false and and_include_keyword_list='" + pattern_group + "' "
                # "and created_at > '2021-09-01' and created_at < '2021-09-30' "
                "order by id desc ", db
            )

            for index, keyword in keyword_list.iterrows():
                # 언급량
                mention_count = pd.read_sql(
                    "select sum(community_count) as community_count, sum(insta_count) as insta_count, "
                    "   sum(blog_count) as blog_count, sum(news_count) as news_count, "
                    "   sum(twitter_count) as twitter_count, sum(count_sum) as count_sum "
                    "from service_mention_counts "
                    "where "
                    "   keyword_id=" + str(keyword["id"]) + " "
                    # "   and term_start >= '2021-07-01' "
                    , db
                )
                # 언급량 전처리.
                if (mention_count.iloc[0]["community_count"] == None) \
                        & (mention_count.iloc[0]["news_count"] == None) \
                        & (mention_count.iloc[0]["count_sum"] == None)\
                        & (mention_count.iloc[0]["insta_count"] == None)\
                        & (mention_count.iloc[0]["blog_count"]==None)\
                        & (mention_count.iloc[0]["twitter_count"]==None):
                    mention_count["community_count"] = 0
                    mention_count["news_count"] = 0
                    mention_count["count_sum"] = 0
                    mention_count["insta_count"] = 0
                    mention_count["blog_count"] = 0
                    mention_count["twitter_count"] = 0

                # 긍부정
                pos_neg_count = pd.read_sql(
                    "select pos_neg, sum(word_count) as word_count from service_pos_neg_words "
                    "where "
                    "   keyword_id=" + str(keyword["id"]) + " and term_type='F' "
                    "group by pos_neg ", db
                )
                # pos_neg_count = pd.read_sql(
                #     "select pos_neg, sum(word_count) as word_count from service_pos_neg_words "
                #     "where "
                #     "   keyword_id=" + str(keyword["id"]) + " and term_type='W' "
                #     "   and term_start >= '2021-07-01' "
                #     "group by pos_neg ", db
                # )
                # 긍부정 전처리.
                pos = 0
                if pos_neg_count[pos_neg_count["pos_neg"] == "POS"].empty == False:
                    pos = pos_neg_count[pos_neg_count["pos_neg"] == "POS"].iloc[0]["word_count"]
                neg = 0
                if pos_neg_count[pos_neg_count["pos_neg"] == "NEG"].empty == False:
                    neg = pos_neg_count[pos_neg_count["pos_neg"] == "NEG"].iloc[0]["word_count"]
                neu = 0
                if pos_neg_count[pos_neg_count["pos_neg"] == "NEU"].empty == False:
                    neu = pos_neg_count[pos_neg_count["pos_neg"] == "NEU"].iloc[0]["word_count"]

                brand_list = ["래미안", "힐스테이트", "자이", "더샵", "푸르지오", "디에이치", "롯데캐슬", "e편한세상", "아이파크", "SK뷰", "꿈에그린", "아크로",
                              "호반써밋", "데시앙", "우미린", "하늘채", "위브", "리슈빌", "어울림", "더휴"]
                if keyword["keyword"] in brand_list:  # 키워드가 브랜드일 경우, 브랜드 파일에, 아닐 경우 회사 파일에.
                    brand_mention_df = brand_mention_df.append(
                        {"키워드": keyword["keyword"], "합계": mention_count.iloc[0]["count_sum"], "커뮤니티": mention_count.iloc[0]["community_count"],
                         "뉴스": mention_count.iloc[0]["news_count"], "인스타": mention_count.iloc[0]["insta_count"],
                         "블로그": mention_count.iloc[0]["blog_count"], "트위터":mention_count.iloc[0]["twitter_count"]
                         }, ignore_index=True
                    )
                    brand_pos_neg_df = brand_pos_neg_df.append(
                        {"키워드": keyword["keyword"], "긍정": pos, "부정": neg, "중립": neu, "공감": (pos + neg + neu)}, ignore_index=True
                    )
                else:
                    company_mention_df = company_mention_df.append(
                        {"키워드": keyword["keyword"], "합계": mention_count.iloc[0]["count_sum"], "커뮤니티": mention_count.iloc[0]["community_count"],
                         "뉴스": mention_count.iloc[0]["news_count"], "인스타": mention_count.iloc[0]["insta_count"],
                         "블로그": mention_count.iloc[0]["blog_count"], "트위터":mention_count.iloc[0]["twitter_count"]}
                        , ignore_index=True
                    )
                    company_pos_neg_df = company_pos_neg_df.append(
                        {"키워드": keyword["keyword"], "긍정": pos, "부정": neg, "중립": neu, "공감": (pos + neg + neu)}, ignore_index=True
                    )
            # 키워드 패턴에 해당하는
            with pd.ExcelWriter("C:/Users/kai/Desktop/두루미/" + pattern_group.replace("\\","_")+ ".xlsx") as excel:
                brand_mention_df.to_excel(excel, sheet_name="브랜드_언급량")
                brand_pos_neg_df.to_excel(excel, sheet_name="브랜드_긍부정")
                company_mention_df.to_excel(excel, sheet_name="회사_언급량")
                company_pos_neg_df.to_excel(excel, sheet_name="회사_긍부정")
                excel.save()


    # social_scraping_data_to_excel()

    def store_excel_data():
        # 몽고디비 연결
        # client = pymongo.MongoClient('localhost', 27017)
        # db = client.noname
        # kospi_list = list(db.kospi_list.find({}))
        kospi_list = pd.read_excel("C:/Users/kai/Desktop/stock_list.xlsx",dtype={"단축코드":"str"})

        # post_id = kospi_list.insert_many(item.to_dict("records"))

        # postgresql 연결
        db = psycopg2.connect(host="112.220.72.179", dbname="openmetric", user="openmetric", password=")!metricAdmin01", port=2345)
        cur = db.cursor()

        # cur.execute("select * from article_post")
        # print(cur.fetchone())

        # kospi_list = kospi_list[1657:]

        # 종목 리스트 반복
        for index, stock in kospi_list.iterrows():
            stock["단축코드"] = "000040"
            stock["한글 종목약명"] = "KR모터스"

            # 이미 인서트 되었는지 확인
            cur.execute("select * from stock_financial_statement where code_id='A"+stock["단축코드"]+"'")
            pre_data = cur.fetchone()
            if pre_data != None:
                continue

            #엑셀 데이터 DataFrame으로 가져오기.
            try:
                # 포괄손익계산서
                pl = pd.read_excel("C:/Users/kai/Downloads/" + stock["한글 종목약명"] + "-포괄손익계산서-분기(3개월)_연결.xlsx",
                                   dtype={"단축코드":"str"})
                # 재무상태표
                bs = pd.read_excel("C:/Users/kai/Downloads/" + stock["한글 종목약명"] + "-재무상태표-분기(3개월)_연결.xlsx",
                                   dtype={"단축코드":"str"})
                # 현금흐름표
                cf = pd.read_excel("C:/Users/kai/Downloads/" + stock["한글 종목약명"] + "-현금흐름표-분기(3개월)_연결.xlsx",
                                   dtype={"단축코드":"str"})

                # dataframe의 컬럼명 변경.
                pl.columns = pl.loc[0]
                pl = pl.drop([0])
                bs.columns = bs.loc[0]
                bs = bs.drop([0])
                cf.columns = cf.loc[0]
                cf = cf.drop([0])

                # corp_code가져오기
                cur.execute("select corp_code from stocks_basic_info where code='A"+str(stock["단축코드"])+"' ")
                corp_code = cur.fetchone()[0]

                def insert_db(df, file_type):
                    insert_sql = ""
                    # 포괄손익계산서 저장.
                    for row in df.index:  # 엑셀 파일의 row들에 대해 반복.
                        # 컬럼에 대해 반복하여 분기를 나타내는 컬럼인 경우에 data insert.
                        for i in df.columns:
                            if None != re.search(r"\d{4}-\d{2}-\d{2}", i):  # yyyy-MM-dd형태의 컬럼명이면 분기데이터 컬럼으로 판단. data insert.
                                # this_term_amount
                                amount = df.loc[row][i]
                                if np.isnan(df.loc[row][i]):
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

                                sql_value = ("("+
                                    "'"+str(datetime.now(timezone.utc))+"', '"+str(datetime.now(timezone.utc))+"', "+
                                    "'"+corp_code+"', '"+i.split("-")[0]+"', '"+i.split("-")[1]+"', '"+i+"', '"+file_type+"', "+
                                    "'"+str(df.loc[row]["account_id"])+"', '"+account_name+"', '"+str(df.loc[row]["LV"])+"', "+
                                    ""+str(amount)+", '"+str(df.loc[row]["LV"])+"', 'A"+str(stock["단축코드"])+"')")
                                insert_sql = insert_sql + ", "+sql_value

                                # cur.execute("INSERT INTO stock_financial_statement ("
                                #             "created_at, updated_at, corp_code, business_year, business_month, this_term_name, "
                                #             "subject_name, account_id, account_name, "
                                #             "account_level, this_term_amount, ordering, code_id) "
                                #             "VALUES ('"+
                                #                 str(datetime.now(timezone.utc)) + "', '" +
                                #                 str( datetime.now(timezone.utc)) + "', "
                                #                 "'" + corp_code + "', '" + i.split("-")[0] + "', '" +
                                #                 i.split("-")[1] + "', '" + i + "', '" + file_type + "', '" +
                                #                 str(df.loc[row]["account_id"]) + "', "
                                #                 "'" + account_name + "', '" + str(df.loc[row]["LV"]) + "', " +
                                #                 str(amount) + ", '" + str(df.loc[row]["LV"]) + "', 'A" + stock["단축코드"] + "') ")

                    insert_sql = insert_sql[1:]
                    # cur.execute(insert_sql)
                    cur.execute("INSERT INTO stock_financial_statement ("
                                "created_at, updated_at, corp_code, business_year, business_month, this_term_name, "
                                "subject_name, account_id, account_name, "
                                "account_level, this_term_amount, ordering, code_id) "
                                "VALUES "+insert_sql)
                    db.commit()

                insert_db(pl, "포괄손익계산서")
                insert_db(bs, "재무상태표")
                insert_db(cf, "현금흐름표")
            except Exception as e:
                date_time = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time()))
                with open("./db_update_fail_list.txt", "a", encoding="UTF-8") as f:
                    f.write(date_time + "_" + str(stock["단축코드"]) + "_" + stock["한글 종목약명"])
                    f.write(traceback.format_exc()+"\n")
                continue

    # store_excel_data()

def noname1():
    db = psycopg2.connect(host="112.220.72.179", dbname="openmetric", user="openmetric", password=")!metricAdmin01",
                          port=2345)
    cur = db.cursor()

    cur.execute(
        "select code_id from stock_financial_statement\
            where code_id not in (select f.code_id from stock_financial_statement as f\
                        where f.account_id='8200'\
                        group by code_id)\
            group by code_id"
    )
    code_list = cur.fetchall()

    for code in code_list:

        cur.execute(
            "select * from stock_financial_statement "
            "where code_id='"+code[0]+"' and subject_name='포괄손익계산서' and (account_id='8110' or account_id='8160')"
        )
        row_list = cur.fetchall() # 기간별 값들.
        insert_sql = ""
        for row in row_list:
            sql_value = ("(" +
                         "'" + str(row[1]) + "', '" + str(row[2]) + "', " +
                         "'" + row[3] + "', '" + row[4] + "', '" + row[5] + "', '" + row[6] + "', '" + row[7] + "', " +
                         "'8200', '총당기순이익', '0', " +
                         "" + str(row[11]) + ", '0', '" + row[13] + "')")
            insert_sql = insert_sql + ", " + sql_value
        insert_sql = insert_sql[1:]
        cur.execute("INSERT INTO stock_financial_statement ("
                    "created_at, updated_at, corp_code, business_year, business_month, this_term_name, "
                    "subject_name, account_id, account_name, "
                    "account_level, this_term_amount, ordering, code_id) "
                    "VALUES " + insert_sql)
        db.commit()
# noname1()

def add_keyword_list(origin, added_keyword_list):
    for added_keyword in added_keyword_list:
        if origin == "":
            origin = added_keyword
        else:
            origin = origin + "\\" + added_keyword
    return origin

def add_dlenc_searchKeyword(keyword_list, include_keyword_list):
    db = psycopg2.connect(host="112.220.72.179", dbname="openmetric", user="openmetric", password=")!metricAdmin01", port=2345)
    cur = db.cursor()

    insert_sql = ""
    for include_keyword_dict in include_keyword_list:
        and_or = include_keyword_dict["and_or"]
        include_keyword_set = include_keyword_dict["include_set"]

        for keyword_set in keyword_list:
            equal_keyword = add_keyword_list("", keyword_set[1])
            exclude_keyword = add_keyword_list("", keyword_set[2])
            or_include_keyword = add_keyword_list("", [""])
            and_include_keyword = add_keyword_list("", [""])
            if and_or == "and":
                and_include_keyword = add_keyword_list("", include_keyword_set)
            elif and_or == "or":
                or_include_keyword = add_keyword_list("", include_keyword_set)
            else:
                raise Exception("and_or 설정 안됨.")
            keyword = keyword_set[0]
            is_followed = "true"
            is_deleted = "false"

            insert_sql = insert_sql + ", (" +\
                         "'" + str(datetime.now(timezone.utc)) + "', '" + str(datetime.now(timezone.utc)) + "', " +\
                         "null, '" + equal_keyword + "', '" + exclude_keyword + "', null, '" + and_include_keyword + "', " +\
                         "'" + or_include_keyword + "', '" + keyword + "', '" + is_followed + "', '" +is_deleted+"', 'sometrend', 'dlenc') "

    insert_sql = insert_sql[1:]
    cur.execute("INSERT INTO social_keywords ("
                "created_at, updated_at, corp_code, equal_keyword_list, exclude_keyword_list, code_id, "
                "and_include_keyword_list, or_include_keyword_list, keyword, is_followed, is_deleted, search_site, use_site"
                ") "
                "VALUES " + insert_sql)
    db.commit()

brand_keyword_set_list = [
    ["e편한세상", [""], ["자이스토리", "스토리"]],
    ["꿈에그린", [""], ["자이스토리", "스토리"]],
    ["더샵", [""], ["자이스토리", "스토리"]],
    ["디에이치", [""], ["자이스토리", "스토리"]],
    ["더플래티넘", [""], ["자이스토리", "스토리"]],
    ["데시앙", [""], ["자이스토리", "스토리"]],
    ["동문굿모닝힐", [""], ["자이스토리", "스토리"]],
    ["동원베네스트", [""], ["자이스토리", "스토리"]],
    ["래미안", [""], ["자이스토리", "스토리"]],
    ["롯데캐슬", [""], ["자이스토리", "스토리"]],
    ["리슈빌", [""], ["자이스토리", "스토리"]],
    ["반도유보라", [""], ["자이스토리", "스토리"]],
    ["베르디움", [""], ["자이스토리", "스토리"]],
    ["벽산블루밍", [""], ["자이스토리", "스토리"]],
    ["삼부르네상스", [""], ["자이스토리", "스토리"]],
    ["서희스타힐스", [""], ["자이스토리", "스토리"]],
    ["센트레빌", [""], ["자이스토리", "스토리"]],
    ["스위첸", [""], ["자이스토리", "스토리"]],
    ["아이파크", [""], ["자이스토리", "스토리"]],
    ["아크로", [""], ["자이스토리", "스토리"]],
    ["금호어울림", [""], ["자이스토리", "스토리"]],
    ["SK뷰", ["에스케이뷰"], ["자이스토리", "스토리"]],
    ["우미린", [""], ["자이스토리", "스토리"]],
    ["위브", [""], ["자이스토리", "스토리"]],
    ["자이", [""], ["자이스토리", "스토리"]],
    ["코아루", [""], ["자이스토리", "스토리"]],
    ["포레나", [""], ["자이스토리", "스토리"]],
    ["푸르지오", [""], ["자이스토리", "스토리"]],
    ["하늘채", [""], ["자이스토리", "스토리"]],
    ["한라비발디", [""], ["자이스토리", "스토리"]],
    ["해링턴플레이스",[""], ["자이스토리", "스토리"]],
    ["해모르", [""], ["자이스토리", "스토리"]],
    ["호반써밋", [""], ["자이스토리", "스토리"]],
    ["힐스테이트", [""], ["자이스토리", "스토리"]],
]
include_keyword_set_list = [
    {"and_or":"and", "include_set":["아파트"]},
    {"and_or":"and", "include_set": ["기술력","아파트"]},
    {"and_or":"and", "include_set": ["디자인","아파트"]},
    {"and_or":"and", "include_set": ["살기좋은","아파트"]},
    {"and_or":"and", "include_set": ["선호하는","아파트"]},
    {"and_or":"and", "include_set": ["스마트","아파트"]},
    {"and_or":"and", "include_set": ["신뢰","아파트"]},
    {"and_or":"and", "include_set": ["입지좋은","아파트"]},
    {"and_or":"and", "include_set": ["추천하는","아파트"]},
    # {"and_or":"and", "include_set": ["]}"친환경"],
    {"and_or":"and", "include_set": ["투자가치","아파트"]},
    {"and_or":"and", "include_set": ["품질좋은","아파트"]},
    # {"and_or":"and", "include_set": ["]}"라이프스타일"],
    {"and_or":"and", "include_set": ["프리미엄","아파트"]},
    {"and_or":"and", "include_set": ["친환경","아파트"]},
    {"and_or":"and", "include_set": ["라이프스타일","아파트"]},
    {"and_or":"or", "include_set": ["설계","평면"]},
    # {"and_or":"and", "include_set": ["]}"디자인"],
    {"and_or":"or", "include_set": ["컨시어지","재택","재택근무","원격교육","원격수업"]},
    {"and_or":"and", "include_set": ["문화","아파트"]},
    {"and_or":"and", "include_set": ["코로나","아파트"]},
    {"and_or":"or", "include_set": ["경험","재택"]},
    {"and_or":"and", "include_set": ["수요","아파트"]},
    {"and_or":"and", "include_set": ["가치","아파트"]},
    {"and_or":"or", "include_set": ["인테리어","벽체","바닥재","마감재"]},
    {"and_or":"or", "include_set": ["가변","펜트리","멀티룸"]},
    {"and_or":"and", "include_set": ["인공지능","아파트"]},
    {"and_or":"or", "include_set": ["홈네트워크","모니터링","공조","제어"]},
    {"and_or":"and", "include_set": ["메타버스","아파트"]},
    {"and_or":"or", "include_set": ["안전","보안","지하화"]},
    # {"and_or":"and", "include_set": ["]}"컨시어지","재택","재택근무","원격교육","원격수업"],
    {"and_or":"or", "include_set": ["소형","초소형"]},
    {"and_or":"and", "include_set": ["재생","아파트"]},
    {"and_or":"or", "include_set": ["로컬","친환경"]},
    {"and_or":"or", "include_set": ["럭셔리","대형평형"]},
    {"and_or":"or", "include_set": ["독립공간","유해요소차단"]},
    {"and_or":"and", "include_set": ["가격상승","아파트"]},
    {"and_or":"and", "include_set": ["개발계획","아파트"]},
    {"and_or":"and", "include_set": ["미래가치","아파트"]},
    # {"and_or":"and", "include_set": ["]}"투자가치"],
    {"and_or":"and", "include_set": ["주거비용","아파트"]},
    # {"and_or":"and", "include_set": ["]}"인테리어","벽체","바닥재","마감재"],
    {"and_or":"or", "include_set": ["소음감소","층간소음저감","소음저감"]},
    {"and_or":"or", "include_set": ["채광","남향"]},
    {"and_or":"or", "include_set": ["환기","에어샤워","공기정화"]},
    {"and_or":"or", "include_set": ["거실","안방","베란다","화장실","주방","알파룸"]},
    {"and_or":"or", "include_set": ["재건축","재개발","호재","개발"]},
    {"and_or":"or", "include_set": ["공원","관공서","녹지","근린시설","병원","마트","숲세권","스세권","올세권","다세권"]},
    {"and_or":"or", "include_set": ["커뮤니티","컨시어지","헬스장","운동","사우나","수영장","볼링장","극장","게스트하우스","도서관","카페","라운지","돌봄"]},
    {"and_or":"or", "include_set": ["대중교통","전철","지하철","교통","역세권"]},
    {"and_or":"or", "include_set": ["교육","학군","학원","초중고","초품아"]},

]

# add_dlenc_searchKeyword(brand_keyword_set_list, include_keyword_set_list)


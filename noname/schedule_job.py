import time

from scrapy import cmdline

def test():
    print("hello")
    # cmdline.execute("mkdir C:/Users/kai/Desktop/erere")
    cmdline.execute("scrapy crawl report_spider".split())

def sector_scraping_check():
    today = time.localtime(time.time())
    if today.tm_mon == 5 & today.tm_mday==18:
        cmdline.execute("scrapy crawl sector_spider -a target_term="+str(today.tm_year)+"-03-31 "
                                                   "-a pre_target_term="+str(today.tm_year-1)+"-12-31".split())
        cmdline.execute("scrapy crawl theme_spider -a target_term=" + str(today.tm_year) + "-03-31 "
                                                                                            "-a pre_target_term=" + str(
            today.tm_year - 1) + "-12-31".split())
    elif today.tm_mon == 8 & today.tm_mday == 17:
        cmdline.execute("scrapy crawl sector_spider -a target_term=" + str(today.tm_year) + "-06-30 "
                                                                                            "-a pre_target_term=" + str(
            today.tm_year) + "-03-31".split())
        cmdline.execute("scrapy crawl theme_spider -a target_term=" + str(today.tm_year) + "-06-30 "
                                                                                            "-a pre_target_term=" + str(
            today.tm_year) + "-03-31".split())
    elif today.tm_mon == 11 & today.tm_mday == 16:
        cmdline.execute("scrapy crawl sector_spider -a target_term=" + str(today.tm_year) + "-09-30 "
                                                                                            "-a pre_target_term=" + str(
            today.tm_year) + "-06-30".split())
        cmdline.execute("scrapy crawl theme_spider -a target_term=" + str(today.tm_year) + "-09-30 "
                                                                                            "-a pre_target_term=" + str(
            today.tm_year) + "-06-30".split())
    elif today.tm_mon == 4 & today.tm_mday == 1:
        cmdline.execute("scrapy crawl sector_spider -a target_term=" + str(today.tm_year-1) + "-12-31 "
                                                                                            "-a pre_target_term=" + str(
            today.tm_year-1) + "-09-30".split())
        cmdline.execute("scrapy crawl theme_spider -a target_term=" + str(today.tm_year - 1) + "-12-31 "
                                                                                                "-a pre_target_term=" + str(
            today.tm_year - 1) + "-09-30".split())

def daily_check():

    today = time.localtime(time.time())
    if today.tm_mon < 4:
        quarterly_date = str(today.tm_year - 1) + "-12-31"
        pre_quarterly_date = str(today.tm_year - 1) + "-09-30"
    elif today.tm_mon < 6:
        quarterly_date = str(today.tm_year) + "-03-31"
        pre_quarterly_date = str(today.tm_year - 1) + "-12-31"
    elif today.tm_mon < 9:
        quarterly_date = str(today.tm_year) + "-06-30"
        pre_quarterly_date = str(today.tm_year) + "-03-31"
    elif today.tm_mon < 12:
        quarterly_date = str(today.tm_year) + "-09-30"
        pre_quarterly_date = str(today.tm_year) + "-06-30"

    # 신규 상장 종목에 대한 과거 재무 데이터 스크래핑.
    # 새로 추가된 종목에 대한 과거 재무 데이터 스크래핑.
    #   이게 선행 되어야, 최신 분기 재무 데이터 스크래핑시, 현재 분기에 대한 중복이 발생하지 않는다.
    # ~~~~~


    # 엑셀에 남은 종목 있나 체크
    # pre_list = pd.read_excel("C:/Users/kai/Desktop/quarterly_data_list_"+pre_quarterly_date+".xlsx")
    # if len(pre_list.index) != 0: # 확인해야 할 데이터가 남아있다면,
    cmdline.execute(("scrapy crawl korean_daily_finance_spider -a quarter="+pre_quarterly_date).split())

    # last_list = pd.read_excel("C:/Users/kai/Desktop/quarterly_data_list_"+quarterly_date+".xlsx")
    # if len(last_list.index) != 0:
    cmdline.execute(("scrapy crawl korean_daily_finance_spider -a quarter="+quarterly_date).split())
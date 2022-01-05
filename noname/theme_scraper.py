import time
from datetime import datetime, date, timedelta

from scrapy import cmdline

# cmdline.execute("scrapy crawl theme_spider -a target_term=2021-06-30 -a pre_target_term=2021-03-31".split())

today = time.localtime(time.time())
if today.tm_mon == 5 & today.tm_mday==18:
    cmdline.execute("scrapy crawl theme_spider -a target_term="+str(today.tm_year)+"-03-31 "
                                               "-a pre_target_term="+str(today.tm_year-1)+"-12-31".split())
elif today.tm_mon == 8 & today.tm_mday == 17:
    cmdline.execute("scrapy crawl theme_spider -a target_term=" + str(today.tm_year) + "-06-30 "
                                                                                        "-a pre_target_term=" + str(
        today.tm_year) + "-03-31".split())
elif today.tm_mon == 11 & today.tm_mday == 16:
    cmdline.execute("scrapy crawl theme_spider -a target_term=" + str(today.tm_year) + "-09-30 "
                                                                                        "-a pre_target_term=" + str(
        today.tm_year) + "-06-30".split())
elif today.tm_mon == 4 & today.tm_mday == 1:
    cmdline.execute("scrapy crawl theme_spider -a target_term=" + str(today.tm_year-1) + "-12-31 "
                                                                                        "-a pre_target_term=" + str(
        today.tm_year-1) + "-09-30".split())
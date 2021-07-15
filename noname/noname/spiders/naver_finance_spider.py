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
        chrome_driver = 'C:/Users/kai/Desktop/chromedriver_win32/chromedriver.exe'
        self.driver = webdriver.Chrome(chrome_driver)

        client = pymongo.MongoClient('localhost', 27017)
        self.db = client.noname
        self.collection = self.db.collection1

    def start_requests(self):
        urls = [
            'https://finance.naver.com/'

        ]

        for url in urls:
            # yield SeleniumRequest(url = url, callback=self.parse)
            yield scrapy.Request(url=url)

    def parse(self, response):
        self.driver.get(response.url)
        # WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.XPATH, "//div[@class='login-page huge']//div[@class='ds input input'][position()=1]/input")))
        self.driver.implicitly_wait(10)

        # 종목 검색
        self.driver.find_element_by_xpath("//div[@id='header']//input[@id='stock_items']").send_keys("005930")
        self.driver.find_element_by_xpath("//div[@id='header']//div[@class='snb_search_box']/button[@alt='검색']").click()

        time.sleep(2)
        self.driver.find_element_by_xpath("//div[@id='wrap']//div[@id='content']//ul[contains(@class,'tabs_submenu')]/li[2]").click()
        time.sleep(2)
        dailyPrice = self.driver.find_element_by_xpath("//div[@id='content']//div[contains(@class,'inner_sub')]/iframe[2]//body//table[@class='type2']")
        dailyPrice_list = []
        print(dailyPrice)
        # for i in
        self.collection.insert_many
        # quote = Quote(sentence=div.css('span.text::text').get(),
        #               authorName=div.css('small.author::text').get(),
        #               authorLink=div.css('span')[1].css('a').attrib['href'])

        # post = {"author": "Mike",
        #     "text": "My first blog post!",
        #     "tags": ["mongodb", "python", "pymongo"],
        #     "date": datetime.datetime.utcnow()}
        # post_id = self.collection.insert_one(post).inserted_id

        dailyPrice.find_element_by_xpath("./body/table[1]//tr[3]")
        # 로그인
        # self.driver.find_element_by_xpath("//div[contains(@class,'login-page')]//div[@class='login-container']//input[@placeholder='계정']").send_keys("tony62@naver.com")
        # self.driver.find_element_by_xpath("//div[contains(@class,'login-page')]//div[@class='login-container']//input[@placeholder='비밀번호']").send_keys("**2TJRGHetc")
        # self.driver.find_element_by_xpath("//div[contains(@class,'login-page')]//div[@class='login-container']//input[@class='button login']").click()
        # time.sleep(3)
        # # 항목 이동
        # menu_bar_button = self.driver.find_element_by_xpath("//div[@class='deepsearch-appbar']//div[contains(@class,'app-bar-drawer')]")
        # self.driver.execute_script("arguments[0].click();", menu_bar_button)
        # self.driver.find_element_by_css_selector('div.drawer-container.slider div.menu-item-group:nth-child(2) div.menu-item:nth-child(5)').click()
        # menu1_button = self.driver.find_element_by_xpath("//div[@class='deepsearch-app']/div[contains(@class,'drawer-container-layout')]/"
        #                                                  "div[contains(@class,'drawer-container')]/div[contains(@class,'drawer-container-inner')]/"
        #                                                  "div[contains(@class,'menu-item-group')][2]/div[@class='menu-item'][2]")
        # self.driver.execute_script("arguments[0].click();", menu1_button)

        # data = self.driver.find_element_by_css_selector('span.table-export-button').click()
        # selector = scrapy.Selector(text=self.driver.page_source)
        # print('result1 : ')
        # print(selector.css("div.rt-td")[0].get())
        # print(data)

        # div = response.css('div.rt-resizable-header-content::text')[0]
        # title = response.css('title::text')
        # quote = Quote(sentence=div.css('span.text::text').get(),
        #               authorName=div.css('small.author::text').get(),
        #               authorLink=div.css('span')[1].css('a').attrib['href'])
        # print('result2 : ')
        # print(title)

        # mongoDB 실습
        # post = {"author": "Mike",
        #     "text": "My first blog post!",
        #     "tags": ["mongodb", "python", "pymongo"],
        #     "date": datetime.datetime.utcnow()}
        # post_id = self.collection.insert_one(post).inserted_id
        # print(post_id)

        # return title

class DailyPrice(scrapy.Item):
    code = scrapy.Field()
    name = scrapy.Field()

    date = scrapy.Field()
    end_price = scrapy.Field()
    yesterday_difference = scrapy.Field()
    start_price = scrapy.Field()
    high_price = scrapy.Field()
    low_price = scrapy.Field()
    exchange_volume = scrapy.Field()

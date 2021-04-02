import scrapy
from sqlobject import *
from sqlobject.sqlbuilder import Insert

from stock_spider import repository
from stock_spider.entitys import WeekPrice
from stock_spider.utils import numberutil


class WeekPriceSpider(scrapy.Spider):
    """
        獲取股票的週Ｋ線資料
        資料來自富邦
    """

    name = "week_price"

    custom_settings = {
        'CONCURRENT_REQUESTS': 1,
        'COOKIES_ENABLED': False,
    }

    def start_requests(self):
        codes = repository.selectAllStockCode()
        for code in codes:
            yield self.createRequest(code.code)

    def createRequest(self, stockCode):
        url = 'https://fubon-ebrokerdj.fbs.com.tw/z/BCD/czkc1.djbcd?a={code}&b=W&c=2880&E=1&ver=5'
        return scrapy.Request(
            url=url.format(code=stockCode),
            callback=self.parse,
            cb_kwargs={'stockCode': stockCode},
        )

    def parse(self, response, stockCode):
        data = response.text
        dataArray = data.split(' ')
        if not self.validate(stockCode, dataArray):
            return

        days = dataArray[0].split(',')
        opening_prices = dataArray[1].split(',')
        highest_prices = dataArray[2].split(',')
        lowest_prices = dataArray[3].split(',')
        closing_prices = dataArray[4].split(',')
        amounts = dataArray[5].split(',')

        daySet = self.selectExistDatesByCode(stockCode)
        for index in range(len(days)):
            # 股票代碼
            code = stockCode
            # 日期
            day = days[index]
            # 開盤價
            opening_price = numberutil.toFloat(opening_prices[index])
            # 最高價
            highest_price = numberutil.toFloat(highest_prices[index])
            # 最低價
            lowest_price = numberutil.toFloat(lowest_prices[index])
            # 收盤價
            closing_price = numberutil.toFloat(closing_prices[index])
            # 成交量
            amount = numberutil.toInt(amounts[index])
            if day not in daySet:
                self.insert(code, day, opening_price, highest_price, lowest_price, closing_price, amount)

    def insert(self, code, day, opening_price, highest_price, lowest_price, closing_price, amount):
        insert = Insert('week_price', values={
            'code': code,
            'day': day,
            'opening_price': opening_price,
            'highest_price': highest_price,
            'lowest_price': lowest_price,
            'closing_price': closing_price,
            'amount': amount
        })
        connection = sqlhub.getConnection()
        query = connection.sqlrepr(insert)
        connection.query(query)

    def selectExistDatesByCode(self, code):
        daySet = set()
        weekPrices = WeekPrice.select(WeekPrice.q.code == code)
        for weekPrice in weekPrices:
            dateStr = weekPrice.day.strftime("%Y/%m/%d")
            daySet.add(dateStr)
        return daySet

    def validate(self, stockCode, dataArray):
        if len(dataArray) != 6:
            self.logger.error('網頁格式錯誤, code=' + stockCode)
            return False
        return True

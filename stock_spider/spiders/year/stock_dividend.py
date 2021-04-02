import scrapy
import pandas as pd
from sqlobject import AND

from stock_spider import repository
from stock_spider.entitys import StockDividend
from stock_spider.utils import numberutil


class StockDividendSpider(scrapy.Spider):
    """
        獲取股票的歷史股利政策
        資料來自富邦
    """

    name = "stock_dividend"

    custom_settings = {
        'CONCURRENT_REQUESTS': 1,
        'COOKIES_ENABLED': False,
    }

    def start_requests(self):
        codes = repository.selectAllStockCode()
        for code in codes:
            yield self.createRequest(code.code)

    def createRequest(self, stockCode):
        url = 'https://fubon-ebrokerdj.fbs.com.tw/z/zc/zcc/zcc_{code}.djhtm'
        return scrapy.Request(
            url=url.format(code=stockCode),
            callback=self.parse,
            cb_kwargs={'stockCode': stockCode},
        )

    def parse(self, response, stockCode):
        dataFrameList = pd.read_html(io=response.text, flavor='bs4')
        if not self.validate(stockCode, dataFrameList):
            return
        dataFrame = dataFrameList[2]
        dataFrame = dataFrame.drop(index=[0, 1, 2, 3])
        for index, row in dataFrame.iterrows():
            # 股票代碼
            code = stockCode
            # 年度
            year = numberutil.toYear(row.get(0))
            # 現金股利
            cash = numberutil.toFloat(row.get(3))
            # 股票股利
            stocks = numberutil.toFloat(row.get(6))
            self.insertIfNotExist(code, year, cash, stocks)

    def insertIfNotExist(self, code, year, cash, stocks):
        count = StockDividend.select(AND(
            StockDividend.q.code == code,
            StockDividend.q.year == year,
        )).count()

        if count == 0:
            StockDividend(
                code=code,
                year=year,
                cash=cash,
                stocks=stocks,
            )

    def validate(self, stockCode, dataFrameList):
        if len(dataFrameList) != 3:
            self.logger.error('網頁格式錯誤, code=' + stockCode)
            return False
        dataFrame = dataFrameList[2]
        testSeries = dataFrame.iloc[3]
        headers = ''
        for column, value in testSeries.items():
            headers = headers + value
        if '股利所屬年度盈餘發放公積發放小計盈餘配股公積配股小計合計員工配股率(%)' != headers:
            self.logger.error('錯誤表頭, code=' + stockCode)
            return False
        return True

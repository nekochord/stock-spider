import scrapy
from scrapy.exceptions import CloseSpider
import pandas as pd
from datetime import *
from datetime import datetime
from dateutil.parser import *
from stock_spider.entitys import *
import stock_spider.repository as repository
import stock_spider.utils.dateutil as dateutil
import stock_spider.utils.numberutil as numberutil
import json
import math


class StockDividendSpider(scrapy.Spider):
    """
    獲取股票的歷史股利政策
    資料來自日盛
    """
    name = "stockDividend"

    custom_settings = {
        'DOWNLOAD_DELAY': 5,
        'CONCURRENT_REQUESTS': 1,
        'COOKIES_ENABLED': False,
    }

    def start_requests(self):
        codes = repository.selectAllStockCode()
        for code in codes:
            yield self.createRequest(code)

    def createRequest(self, stockCode):
        url = 'http://jsjustweb.jihsun.com.tw/z/zc/zcc/zcc_{code}.djhtm'
        return scrapy.Request(
            url=url.format(code=stockCode.code),
            callback=self.parse,
            cb_kwargs={'stockCode': stockCode}
        )

    def parse(self, response, stockCode):
        dataFrameList = pd.read_html(response.text, flavor='bs4')
        if len(dataFrameList) != 3:
            self.logger.error('網頁格式錯誤')
            return
        dataFrame = dataFrameList[2]
        self.validateDataFrame(dataFrame)
        dataFrame = dataFrame.drop(index=[0, 1, 2])
        dataFrame = dataFrame.T
        dataFrame = dataFrame.set_index(3)
        dataFrame = dataFrame.T
        dataFrame = dataFrame.set_index('股利所屬年度')
        todayYear = date.today().year
        for index, row in dataFrame.iterrows():
            year = numberutil.toYear(index)
            if year == None or todayYear == year:
                continue
            numRow = pd.to_numeric(row, downcast='float', errors='coerce')
            numRow = numRow.fillna(0)
            cash = numRow.get('盈餘發放') + numRow.get('公積發放')
            stocks = numRow.get('盈餘配股') + numRow.get('公積配股')
            self.insertIfNotExist(stockCode.code, year, cash, stocks)

    def insertIfNotExist(self, code, year, cash, stocks):

        count = StockDividend.select(AND(
            StockDividend.q.code == code,
            StockDividend.q.year == year
        )).count()

        if count == 0:
            StockDividend(
                code=code,
                year=year,
                cash=cash,
                stocks=stocks
            )

    # 驗證表頭是否正確
    def validateDataFrame(self, dataFrame):
        testSeries = dataFrame.iloc[3]
        headers = ''
        for column, value in testSeries.items():
            headers = headers + value

        if '股利所屬年度盈餘發放公積發放小計盈餘配股公積配股小計合計員工配股率(%)' != headers:
            raise CloseSpider('錯誤表頭')

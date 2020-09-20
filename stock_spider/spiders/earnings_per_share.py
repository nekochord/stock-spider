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


class EarningsPerShareSpider(scrapy.Spider):
    """
    獲取股票的歷史本益比
    資料來自嗨財報
    """
    name = "eps"

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
        url = 'https://histock.tw/stock/{code}/%E6%AF%8F%E8%82%A1%E7%9B%88%E9%A4%98'
        return scrapy.Request(
            url=url.format(code=stockCode.code),
            callback=self.parse,
            cb_kwargs={'stockCode': stockCode}
        )

    def parse(self, response, stockCode):
        dataFrameList = pd.read_html(response.text)
        if len(dataFrameList) < 1:
            self.logger.error('網頁格式錯誤')
            return

        dataFrame = dataFrameList[0]
        edit = self.validateDataFrameColumns(dataFrame)
        for name, column in edit.items():
            year = numberutil.toYear(name)
            if year == None:
                continue
            numColumn = pd.to_numeric(column, errors='coerce')
            for index, value in numColumn.items():
                if (index == 'Q1'):
                    self.insertIfNotExist(stockCode.code, year, 1, value)
                if (index == 'Q2'):
                    self.insertIfNotExist(stockCode.code, year, 2, value)
                if (index == 'Q3'):
                    self.insertIfNotExist(stockCode.code, year, 3, value)
                if (index == 'Q4'):
                    self.insertIfNotExist(stockCode.code, year, 4, value)

    def insertIfNotExist(self, code, year, quarter, eps):
        if math.isnan(eps):
            return

        count = EarningsPerShare.select(AND(
            EarningsPerShare.q.code == code,
            EarningsPerShare.q.year == year,
            EarningsPerShare.q.quarter == quarter
        )).count()

        if count == 0:
            EarningsPerShare(
                code=code,
                year=year,
                quarter=quarter,
                eps=eps
            )

    # 驗證表頭是否正確
    def validateDataFrameColumns(self, dataFrame):
        seasonYear = dataFrame.columns[0]
        if seasonYear != '季別/年度':
            self.logger.error('錯誤表頭=' + seasonYear)
            raise CloseSpider('錯誤表頭')

        edit = dataFrame.set_index('季別/年度')
        headers = ''
        for index in edit.index:
            headers = headers + str(index)

        if headers != 'Q1Q2Q3Q4總計':
            self.logger.error('錯誤表頭=' + headers)
            raise CloseSpider('錯誤表頭')
        return edit

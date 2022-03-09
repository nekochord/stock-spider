import scrapy
import pandas as pd
from sqlobject import *
from sqlobject.sqlbuilder import Insert

from stock_spider import repository
from stock_spider.entitys import DayInstitutionalInvestor
from stock_spider.utils import numberutil


class DayInstitutionalInvestorSpider(scrapy.Spider):
    """
        獲取股票的每日三大法人交易資料
        資料來自富邦
    """

    name = "day_institutional_investor"

    custom_settings = {
        'CONCURRENT_REQUESTS': 1,
        'COOKIES_ENABLED': False,
    }

    def start_requests(self):
        codes = repository.selectAllStockCode()
        for code in codes:
            yield self.createRequest(code.code)

    def createRequest(self, stockCode):
        url = 'https://fubon-ebrokerdj.fbs.com.tw/z/zc/zcl/zcl_{code}.djhtm'
        return scrapy.Request(
            url=url.format(code=stockCode),
            callback=self.parse,
            cb_kwargs={'stockCode': stockCode},
        )

    def parse(self, response, stockCode):
        dataFrameList = pd.read_html(response.text)

        if not self.validate(stockCode, dataFrameList):
            return

        daySet = self.selectExistDatesByCode(stockCode)
        dataFrame = dataFrameList[2]
        for index, row in dataFrame.iterrows():
            if 7 <= index <= 11:
                code = stockCode
                # 日期
                sp = row.get(0).split('/')
                year = numberutil.toInt(sp[0]) + 1911
                day = str(year) + '/' + sp[1] + '/' + sp[2]
                # 外資
                foreign = numberutil.toInt(row.get(1))
                # 投信
                investment_trust = numberutil.toInt(row.get(2))
                # 自營商
                dealer = numberutil.toInt(row.get(3))
                if day not in daySet:
                    self.insert(code, day, '外資', foreign)
                    self.insert(code, day, '投信', investment_trust)
                    self.insert(code, day, '自營商', dealer)

    def insert(self, code, day, investor, total):
        insert = Insert('day_institutional_investor', values={
            'code': code,
            'day': day,
            'investor': investor,
            'total': total
        })
        connection = sqlhub.getConnection()
        query = connection.sqlrepr(insert)
        connection.query(query)

    def selectExistDatesByCode(self, code):
        daySet = set()
        investors = DayInstitutionalInvestor.select(DayInstitutionalInvestor.q.code == code)
        for investor in investors:
            dateStr = investor.day.strftime("%Y/%m/%d")
            daySet.add(dateStr)
        return daySet

    def validate(self, stockCode, dataFrameList):
        if len(dataFrameList) != 4:
            self.logger.error('網頁格式錯誤, code=' + stockCode)
            return False
        dataFrame = dataFrameList[2]
        testSeries = dataFrame.iloc[6]
        headers = ''
        for column, value in testSeries.items():
            headers = headers + str(value)
        if '日期外資投信自營商單日合計外資投信自營商單日合計外資三大法人' != headers:
            self.logger.error('錯誤表頭, code=' + stockCode)
            return False
        return True

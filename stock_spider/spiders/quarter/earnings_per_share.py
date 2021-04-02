import scrapy
import pandas as pd
from sqlobject import AND

from stock_spider import repository
from stock_spider.entitys import EarningsPerShare
from stock_spider.utils import numberutil


class EarningsPerShareSpider(scrapy.Spider):
    """
        獲取股票的歷史本益比(季報)
        只會包含前五年的資料
        資料來自富邦
    """

    name = "eps"

    custom_settings = {
        'CONCURRENT_REQUESTS': 1,
        'COOKIES_ENABLED': False,
    }

    def start_requests(self):
        codes = repository.selectAllStockCode()
        for code in codes:
            yield self.createRequest(code.code)

    def createRequest(self, stockCode):
        url = 'https://fubon-ebrokerdj.fbs.com.tw/z/zc/zcd_{code}.djhtm'
        return scrapy.Request(
            url=url.format(code=stockCode),
            callback=self.parse,
            cb_kwargs={'stockCode': stockCode},
        )

    def parse(self, response, stockCode):
        dataFrameList = pd.read_html(response.text)
        if not self.validate(stockCode, dataFrameList):
            return
        dataFrame = dataFrameList[2]
        dataFrame = dataFrame.drop(index=[0, 1])
        for index, row in dataFrame.iterrows():
            yearQuarter = row.get(0)
            # 年度
            year = numberutil.toInt(yearQuarter.split('.')[0]) + 1911
            # 季別
            quarter = numberutil.toInt(yearQuarter.split('.')[1].replace('Q', ''))
            # 加權平均股數 (千股)
            number_of_shares = numberutil.toInt(row.get(1))
            # 稅前淨利 (百萬元)
            pre_tax_income = numberutil.toInt(row.get(3))
            # 稅後淨利 (百萬元)
            net_income = numberutil.toInt(row.get(4))
            # 稅前每股盈餘(元)
            pre_tax_eps = numberutil.toFloat(row.get(6))
            # 稅後每股盈餘(元)
            eps = numberutil.toFloat(row.get(7))
            self.insertIfNotExist(stockCode, year, quarter, number_of_shares, pre_tax_income, net_income, pre_tax_eps,
                                  eps)

    def insertIfNotExist(self, code, year, quarter, number_of_shares, pre_tax_income, net_income, pre_tax_eps, eps):
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
                number_of_shares=number_of_shares,
                pre_tax_income=pre_tax_income,
                net_income=net_income,
                pre_tax_eps=pre_tax_eps,
                eps=eps,
            )

    def validate(self, stockCode, dataFrameList):
        if len(dataFrameList) != 4:
            self.logger.error('網頁格式錯誤, code=' + stockCode)
            return False
        dataFrame = dataFrameList[2]
        testSeries = dataFrame.iloc[1]
        headers = ''
        for column, value in testSeries.items():
            headers = headers + value
        if '季別加權平均股數營業收入稅前淨利稅後淨利每股營收(元)稅前每股盈餘(元)稅後每股盈餘(元)' != headers:
            self.logger.error('錯誤表頭, code=' + stockCode)
            return False
        return True

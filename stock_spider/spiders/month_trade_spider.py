import scrapy
from scrapy.exceptions import CloseSpider
import pandas as pd
from datetime import *
from dateutil.parser import *
from stock_spider.entitys import *
import stock_spider.repository as repository
import stock_spider.utils.dateutil as dateutil
import stock_spider.utils.numberutil as numberutil
import json


class MonthTradeSpider(scrapy.Spider):
    """
    獲取股票的月成交資訊
    會去DB查看哪一年有漏掉
    """
    name = "monthTrade"

    custom_settings = {
        'DOWNLOAD_DELAY': 5,
        'CONCURRENT_REQUESTS': 1,
        'COOKIES_ENABLED': False,
    }

    def __init__(self,  *args, **kwargs):
        super(MonthTradeSpider, self).__init__(*args, **kwargs)
        # 清掉可能需要更新的年份
        today = date.today()
        codes = repository.selectAllStockCode()

        for code in codes:
            # 現在是1月看去年12月有沒有抓
            if (today.month == 1 and
                    not self.existByCodeAndYearAndMonth(code.code, today.year-1, 12)):
                self.log('清掉去年資料 code={code} year={year}'.format(
                    code=code.code, year=str(today.year-1)))
                self.deleteByYear(code.code, today.year-1)
            # 看上個月有沒有抓
            if (today.month > 1 and
                    not self.existByCodeAndYearAndMonth(code.code, today.year, today.month-1)):
                self.log('清掉今年資料 code={code} year={year}'.format(
                    code=code.code, year=str(today.year)))
                self.deleteByYear(code.code, today.year)

    def start_requests(self):
        # 判斷過去10年有哪幾年是完全沒資料的然後重抓
        today = date.today()
        tenYearBeforeDate = today.replace(year=today.year-10)
        codes = repository.selectAllStockCode()

        for code in codes:
            if(tenYearBeforeDate > code.issuanceDate):
                startDate = tenYearBeforeDate
            else:
                startDate = code.issuanceDate

            yearGen = dateutil.yearGenerator(startDate=startDate)
            for yearDate in yearGen:
                # 如果那年有資料就不抓
                if self.countByCodeAndYear(code.code, yearDate.year) > 0:
                    continue

                yield self.createRequest(code, yearDate)

    def createRequest(self, stockCode, reqDate):
        urlTemp = 'https://www.twse.com.tw/en/exchangeReport/FMSRFK?response=json&date={date}&stockNo={code}'
        dateStr = reqDate.strftime("%Y%m%d")
        return scrapy.Request(
            url=urlTemp.format(date=dateStr, code=stockCode.code),
            callback=self.parse,
            cb_kwargs={'code': stockCode.code}
        )

    def parse(self, response, code):
        JSON = response.json()
        self.validateData(JSON)
        data = JSON['data']
        for record in data:
            MonthTrade(
                code=code,
                year=record[0],
                month=record[1],
                highestPrice=numberutil.toFloat(record[2]),
                lowestPrice=numberutil.toFloat(record[3]),
                weightedAveragePrice=numberutil.toFloat(record[4]),
                transactions=numberutil.toInt(record[5]),
                tradeValue=numberutil.toInt(record[6]),
                tradeVolume=numberutil.toInt(record[7]),
                turnoverRatio=numberutil.toFloat(record[8]),
            )

    def countByCodeAndYear(self, code, year):
        return MonthTrade.select(AND(
            MonthTrade.q.code == code,
            MonthTrade.q.year == year
        )).count()

    def existByCodeAndYearAndMonth(self, code, year, month):
        return MonthTrade.select(AND(
            MonthTrade.q.code == code,
            MonthTrade.q.year == year,
            MonthTrade.q.month == month
        )).count() > 0

    def deleteByYear(self, code, year):
        MonthTrade.deleteMany(AND(
            MonthTrade.q.code == code,
            MonthTrade.q.year == year
        ))

    def validateData(self, JSON):
        assertFields = [
            "Year",
            "Month",
            "Highest Price",
            "Lowest Price",
            "Weighted Average Price (A/B)",
            "Transaction",
            "Trade Value (A)",
            "Trade Volume(B)",
            "Turnover Ratio(%)"]
        fields = JSON['fields']
        for i in range(len(assertFields)):
            if(assertFields[i] != fields[i]):
                self.log('錯誤表頭='+str(fields))
                raise CloseSpider('錯誤表頭')

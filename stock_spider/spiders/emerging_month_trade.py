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


class EmergingMonthTradeSpider(scrapy.Spider):
    """
    獲取上櫃股票的月成交資訊
    會去DB查看哪一年有漏掉
    """
    name = "emergingMonthTrade"

    custom_settings = {
        'DOWNLOAD_DELAY': 5,
        'CONCURRENT_REQUESTS': 1,
        'COOKIES_ENABLED': False,
    }

    def __init__(self,  *args, **kwargs):
        super(EmergingMonthTradeSpider, self).__init__(*args, **kwargs)
        # 清掉可能需要更新的年份
        today = date.today()
        codes = repository.selectEmergingStockCode()

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
        codes = repository.selectEmergingStockCode()

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
        url = 'https://www.tpex.org.tw/web/stock/statistics/monthly/result_st44.php?l=en-us'
        url = url+'&timestamp='+str(datetime.now().timestamp())
        yearStr = reqDate.strftime("%Y")

        return scrapy.FormRequest(
            method='POST',
            url=url,
            formdata={'yy': yearStr, 'input_stock_code': stockCode.code},
            callback=self.parse,
            cb_kwargs={'year': yearStr, 'code': stockCode.code},
        )

    def parse(self, response, year, code):
        dataFrameList = pd.read_html(response.text)
        if(len(dataFrameList) < 2):
            self.log('頁面格式錯誤, year={year} code={code}'.format(
                year=year, code=code
            ))
            return

        dataFrame = dataFrameList[2]
        self.validateDataFrameColumns(year, code, dataFrame)
        for row in dataFrame.itertuples():
            MonthTrade(
                code=code,
                year=row[1],
                month=row[2],
                highestPrice=self.toFloat(row[3]),
                lowestPrice=self.toFloat(row[4]),
                weightedAveragePrice=self.toFloat(row[5]),
                transactions=row[6],
                tradeValue=row[7]*1000,
                tradeVolume=row[8]*1000,
                turnoverRatio=self.toFloat(row[9]),
            )

    def toFloat(self, num):
        if(type(num) == float):
            return num
        if (type(num) == str):
            try:
                return numberutil.toFloat(num)
            except Exception:
                self.log('錯誤浮點數='+num)
        else:
            self.log('錯誤浮點數='+str(num))
        return None

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

     # 驗證表頭是否正確
    def validateDataFrameColumns(self, year, code, dataFrame):
        headerCombine = ''
        for column in dataFrame.columns:
            headerCombine = headerCombine+column+','

        assertHeaders = 'Year,Month,Highestprice,Lowestprice,Averageclosingprice,Number oftransactions,TradingValue(NTD, in thousands) (A),Numbershares(in thousands) (B),Turnoverratio (%),'
        if(assertHeaders != headerCombine):
            raise Exception('錯誤表頭, year={year} code={code} '.format(
                code=code, year=year))
        else:
            self.log('抓取成功, year={year} code={code}'.format(
                code=code, year=year))

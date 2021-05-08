import scrapy
import pandas as pd

from stock_spider import repository
from stock_spider.entitys import MonthlyRevenue
from stock_spider.utils import numberutil


class MonthlyRevenueSpider(scrapy.Spider):
    """
        獲取股票的每月營收
        資料來自富邦
    """

    name = "monthly_revenue"

    custom_settings = {
        'CONCURRENT_REQUESTS': 1,
        'COOKIES_ENABLED': False,
    }

    def start_requests(self):
        codes = repository.selectAllStockCode()
        for code in codes:
            yield self.createRequest(code.code)

    def createRequest(self, stockCode):
        url = 'https://fubon-ebrokerdj.fbs.com.tw/z/zc/zch/zch_{code}.djhtm'
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
        dataFrame = dataFrame.drop(index=[0, 1, 2, 3, 4, 5])
        monthSet = self.selectExistByCode(stockCode)
        for index, row in dataFrame.iterrows():
            # 股票代碼
            code = stockCode
            # 年度
            year = numberutil.toInt(row.get(0).split('/')[0]) + 1911
            # 月
            month = numberutil.toInt(row.get(0).split('/')[1])
            # 營收 (仟元)
            operating_revenue = numberutil.toInt(row.get(1))
            # 月增率 (百分比)
            mom = numberutil.toFloat(row.get(2))
            # 去年同期 (仟元)
            same_month_last_year = numberutil.toInt(row.get(3))
            # 月營收年增率 (百分比)
            yoy = numberutil.toFloat(row.get(4))
            # 累計營收 (仟元)
            cumulative_revenue = numberutil.toInt(row.get(5))
            # 累計營收年增率 (百分比)
            cumulative_revenue_yoy = numberutil.toFloat(row.get(6))
            if self.monthStr(year, month) not in monthSet:
                self.insert(code, year, month, operating_revenue, mom, same_month_last_year,
                            yoy, cumulative_revenue, cumulative_revenue_yoy)

    def insert(self, code, year, month, operating_revenue, mom, same_month_last_year,
               yoy, cumulative_revenue, cumulative_revenue_yoy):
        MonthlyRevenue(
            code=code,
            year=year,
            month=month,
            operating_revenue=operating_revenue,
            mom=mom,
            same_month_last_year=same_month_last_year,
            yoy=yoy,
            cumulative_revenue=cumulative_revenue,
            cumulative_revenue_yoy=cumulative_revenue_yoy,
        )

    def selectExistByCode(self, code):
        monthSet = set()
        revenues = MonthlyRevenue.select(MonthlyRevenue.q.code == code)
        for revenue in revenues:
            monthStr = self.monthStr(revenue.year, revenue.month)
            monthSet.add(monthStr)
        return monthSet

    def monthStr(self, year, month):
        return '{year}#{month}'.format(year=year, month=month)

    def validate(self, stockCode, dataFrameList):
        if len(dataFrameList) != 3:
            self.logger.error('網頁格式錯誤, code=' + stockCode)
            return False
        dataFrame = dataFrameList[2]
        testSeries = dataFrame.iloc[5]
        headers = ''
        for column, value in testSeries.items():
            headers = headers + str(value)
        if '年/月營收月增率去年同期年增率累計營收年增率nan' != headers:
            self.logger.error('錯誤表頭, code=' + stockCode)
            return False
        return True

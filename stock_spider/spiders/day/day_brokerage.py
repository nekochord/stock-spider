import scrapy
import pandas as pd
import datetime
from sqlobject import *
from sqlobject.sqlbuilder import Insert

from stock_spider.entitys import DayPrice, BrokerageName
from stock_spider.utils import numberutil


class DayBrokerageSpider(scrapy.Spider):
    """
        獲取股票的每日主力買賣超排行資料
        資料來自富邦
    """

    name = "day_brokerage"

    custom_settings = {
        'CONCURRENT_REQUESTS': 1,
        'COOKIES_ENABLED': False,
    }

    # 初始化方法
    def __init__(self, *args, **kwargs):
        super(DayBrokerageSpider, self).__init__(*args, **kwargs)
        self.brokerage_dict = self.selectBrokerageNameToIdDict()

    def start_requests(self):
        today = datetime.date.fromisoformat('2022-04-22')
        allDayPrice = self.selectAllDayPrice(today)
        for dayPrice in allDayPrice:
            if dayPrice.amount > 0:
                yield self.createRequest(dayPrice.code, today)

    def createRequest(self, stockCode, today):
        day = today.strftime("%Y-%m-%d")
        url = 'https://fubon-ebrokerdj.fbs.com.tw/z/zc/zco/zco.djhtm?a={code}&e={day}&f={day}'
        return scrapy.Request(
            url=url.format(code=stockCode, day=day),
            callback=self.parse,
            cb_kwargs={'stockCode': stockCode, 'today': today},
        )

    def parse(self, response, stockCode, today):
        dataFrameList = pd.read_html(response.text)

        if not self.validate(stockCode, response.text, dataFrameList):
            return

        dataFrame = dataFrameList[2]
        dataFrame = dataFrame.drop(index=[0, 1, 2, 3, 4, 5])

        day = today.strftime("%Y/%m/%d")
        for index, row in dataFrame.iterrows():
            if index >= 6:
                first = row.get(0)
                if '合計買超張數' == first:
                    break

                # 股票代碼
                code = stockCode
                # 買超券商名稱
                buy_brokerage_name = row.get(0)
                if pd.isna(buy_brokerage_name):
                    break
                # 買超量
                buy_total = numberutil.toInt(row.get(3))
                # 買超比重
                buy_percent = numberutil.toFloat(row.get(4))
                # 賣超券商名稱
                sell_brokerage_name = row.get(5)
                if pd.isna(sell_brokerage_name):
                    break
                # 賣超量
                sell_total = numberutil.toInt(row.get(8))
                # 賣超比重
                sell_percent = numberutil.toFloat(row.get(9))
                self.insert(code, day, buy_brokerage_name, buy_total, buy_percent)
                self.insert(code, day, sell_brokerage_name, -sell_total, sell_percent)

    def insert(self, code, day, brokerage_name, total, percent):
        brokerage_id = self.brokerage_dict.get(brokerage_name)

        if brokerage_id is None:
            new_brokerage = BrokerageName(brokerage_name=brokerage_name)
            brokerage_id = new_brokerage.id
            self.brokerage_dict[brokerage_name] = brokerage_id

        insert = Insert('day_brokerage_activity', values={
            'code': code,
            'day': day,
            'brokerage_id': brokerage_id,
            'total': total,
            'percent': percent
        })
        connection = sqlhub.getConnection()
        query = connection.sqlrepr(insert)
        connection.query(query)

    def selectAllDayPrice(self, today):
        return DayPrice.select(DayPrice.q.day == today).orderBy('code')

    def selectBrokerageNameToIdDict(self):
        all_name = BrokerageName.select()
        name_to_id_dict = dict()
        for name in all_name:
            name_to_id_dict[name.brokerage_name] = name.id
        return name_to_id_dict

    def validate(self, stockCode, responseText, dataFrameList):
        if "查無" in responseText:
            self.logger.error('查無資料, code=' + stockCode)
            return False
        if len(dataFrameList) != 4:
            self.logger.error('網頁格式錯誤, code=' + stockCode)
            return False
        dataFrame = dataFrameList[2]
        testSeries = dataFrame.iloc[5]
        headers = ''
        for column, value in testSeries.items():
            headers = headers + str(value)
        if '買超券商買進賣出買超佔成交比重賣超券商買進賣出賣超佔成交比重' != headers:
            self.logger.error('錯誤表頭, code=' + stockCode)
            return False
        return True

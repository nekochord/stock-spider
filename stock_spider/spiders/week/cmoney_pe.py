import scrapy
import json
from stock_spider import repository
from sqlobject import *
from stock_spider.entitys import CmoneyPe
from stock_spider.utils import numberutil


class CMoneyPeSpider(scrapy.Spider):
    """
        獲取股票的法人預估本益比資料
        資料來自CMoney
    """

    name = "cmoney_pe"

    custom_settings = {
        'CONCURRENT_REQUESTS': 1,
        'COOKIES_ENABLED': False,
    }

    def start_requests(self):
        codes = repository.selectAllStockCode()
        for code in codes:
            yield self.createRequest(code.code)

    def createRequest(self, stockCode):
        url = 'https://www.cmoney.tw/finance/ashx/mainpage.ashx?stockId={code}&action=GetPERAndEPSBySeason&cmkey=KcQOrThjFRHrBSk+q7vUVA=='
        return scrapy.Request(
            url=url.format(code=stockCode),
            headers={"Referer": "https://www.cmoney.tw/finance/f00032.aspx"},
            callback=self.parse,
            cb_kwargs={'stockCode': stockCode},
        )

    def parse(self, response, stockCode):
        json_str = response.text
        if not self.validate(json_str):
            self.logger.error('回應格式錯誤, code=' + stockCode)
            return
        data = json.loads(json_str)
        monthSet = self.selectExistByCode(stockCode)
        for record in data:
            SeasonDate = record['SeasonDate']
            # 股票代碼
            code = stockCode
            # 年度
            year = numberutil.toYear(SeasonDate[:4])
            # 月
            month = numberutil.toInt(SeasonDate[4:])
            # 法人預估本益比
            per = numberutil.toFloat(record['PER'])
            # 本益比(季高)
            per_season_high = numberutil.toFloat(record['PERSeasonHigh'])
            # 本益比(季低)
            per_season_low = numberutil.toFloat(record['PERSeasonLow'])
            # 本益比(近4季)
            per_by_twse = numberutil.toFloat(record['PERByTWSE'])

            if self.monthStr(year, month) not in monthSet:
                self.insert(code, year, month, per, per_season_high, per_season_low, per_by_twse)

    def insert(self, code, year, month, per, per_season_high, per_season_low, per_by_twse):
        CmoneyPe(
            code=code,
            year=year,
            month=month,
            per=per,
            per_season_high=per_season_high,
            per_season_low=per_season_low,
            per_by_twse=per_by_twse
        )

    def selectExistByCode(self, code):
        monthSet = set()
        pes = CmoneyPe.select(CmoneyPe.q.code == code)
        for pe in pes:
            monthStr = self.monthStr(pe.year, pe.month)
            monthSet.add(monthStr)
        return monthSet

    def monthStr(self, year, month):
        return '{year}#{month}'.format(year=year, month=month)

    def validate(self, json_str):
        try:
            data = json.loads(json_str)
            record = data[0]
            if 'SeasonDate' not in record:
                return False
            if 'PERSeasonHigh' not in record:
                return False
            if 'PERSeasonLow' not in record:
                return False
            if 'PER' not in record:
                return False
            if 'PERByTWSE' not in record:
                return False
            return True
        except Exception:
            return False

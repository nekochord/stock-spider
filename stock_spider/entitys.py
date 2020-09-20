import os
from sqlobject import *

# 設定 SQLObject 的連線
connection_string = 'sqlite:'+os.path.abspath('SQLite/stock_spider.db')
sqlhub.processConnection = connectionForURI(connection_string)


class StockCode(SQLObject):
    """
    股票代碼資訊
    """
    # 股票代碼
    code = col.StringCol()
    # 有價證券名稱
    name = col.StringCol()
    # 市場別
    marketType = col.StringCol()
    # 有價證券別
    stockType = col.StringCol()
    # 產業別
    industryType = col.StringCol()
    # 公開發行日
    issuanceDate = col.DateCol()


# 自動創建 Table
StockCode.createTable(ifNotExists=True)


class MonthTrade(SQLObject):
    """
    個股月成交資訊
    """
    # 股票代碼
    code = col.StringCol()
    # 年度
    year = col.IntCol()
    # 月份
    month = col.IntCol()
    # 最高價
    highestPrice = col.FloatCol()
    # 最低價
    lowestPrice = col.FloatCol()
    # 加權平均價
    weightedAveragePrice = col.FloatCol()
    # 成交筆數
    transactions = col.BigIntCol()
    # 成交金額
    tradeValue = col.BigIntCol()
    # 成交股數
    tradeVolume = col.BigIntCol()
    # 週轉率
    turnoverRatio = col.FloatCol()


MonthTrade.createTable(ifNotExists=True)


class EarningsPerShare(SQLObject):
    """
    個股的歷史EPS
    """
    # 股票代碼
    code = col.StringCol()
    # 年度
    year = col.IntCol()
    # 季別
    quarter = col.IntCol()
    # EPS
    eps = col.FloatCol()


EarningsPerShare.createTable(ifNotExists=True)


class StockDividend(SQLObject):
    """
    歷年股利
    """
    # 股票代碼
    code = col.StringCol()
    # 年度
    year = col.IntCol()
    # 現金股利
    cash = col.FloatCol()
    # 股票股利
    stocks = col.FloatCol()


StockDividend.createTable(ifNotExists=True)

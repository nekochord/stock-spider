from sqlobject import *
from stock_spider.environment import properties

# 設定 SQLObject 的連線
sqlhub.processConnection = connectionForURI(properties['db.url'])


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
    # 加權平均股數 (千股)
    number_of_shares = col.BigIntCol()
    # 稅前淨利 (百萬元)
    pre_tax_income = col.BigIntCol()
    # 稅後淨利 (百萬元)
    net_income = col.BigIntCol()
    # 稅前每股盈餘(元)
    pre_tax_eps = col.FloatCol()
    # 稅後每股盈餘(元)
    eps = col.FloatCol()


EarningsPerShare.createTable(ifNotExists=True)


class StockDividend(SQLObject):
    """
    歷年股利政策
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


class ProfitAnalysis(SQLObject):
    """
    個股獲利能力分析
    """
    # 股票代碼
    code = col.StringCol()
    # 年度
    year = col.IntCol()
    # 季別
    quarter = col.IntCol()
    # 營業收入 (百萬元)
    operating_revenue = col.BigIntCol()
    # 營業成本 (百萬元)
    operating_cost = col.BigIntCol()
    # 營業毛利 (百萬元)
    gross_profit = col.BigIntCol()
    # 毛利率 (百分比)
    gross_profit_margin = col.FloatCol()
    # 營業利益 (百萬元)
    operating_profit = col.BigIntCol()
    # 營益率 (百分比)
    operating_profit_margin = col.FloatCol()
    # 業外收支 (百萬元)
    non_operating_revenue = col.BigIntCol()
    # 稅前淨利 (百萬元)
    pre_tax_income = col.BigIntCol()
    # 稅後淨利 (百萬元)
    net_income = col.BigIntCol()


ProfitAnalysis.createTable(ifNotExists=True)


class MonthlyRevenue(SQLObject):
    """
    個股每月營收
    """
    # 股票代碼
    code = col.StringCol()
    # 年度
    year = col.IntCol()
    # 月
    month = col.IntCol()
    # 營收 (仟元)
    operating_revenue = col.BigIntCol()
    # 月增率 (百分比)
    mom = col.FloatCol()
    # 去年同期 (仟元)
    same_month_last_year = col.BigIntCol()
    # 月營收年增率 (百分比)
    yoy = col.FloatCol()
    # 累計營收 (仟元)
    cumulative_revenue = col.BigIntCol()
    # 累計營收年增率 (百分比)
    cumulative_revenue_yoy = col.FloatCol()


MonthlyRevenue.createTable(ifNotExists=True)


class DayPrice(SQLObject):
    """
    個股每日價格
    """
    # 股票代碼
    code = col.StringCol()
    # 日期
    day = col.DateCol(dateFormat='%Y/%m/%d')
    # 開盤價
    opening_price = col.FloatCol()
    # 最高價
    highest_price = col.FloatCol()
    # 最低價
    lowest_price = col.FloatCol()
    # 收盤價
    closing_price = col.FloatCol()
    # 成交量
    amount = col.BigIntCol()


DayPrice.createTable(ifNotExists=True)


class WeekPrice(SQLObject):
    """
    個股每週價格
    """
    # 股票代碼
    code = col.StringCol()
    # 日期
    day = col.DateCol(dateFormat='%Y/%m/%d')
    # 開盤價
    opening_price = col.FloatCol()
    # 最高價
    highest_price = col.FloatCol()
    # 最低價
    lowest_price = col.FloatCol()
    # 收盤價
    closing_price = col.FloatCol()
    # 成交量
    amount = col.BigIntCol()


WeekPrice.createTable(ifNotExists=True)


class CmoneyPe(SQLObject):
    """
    CMoney本益比資料
    """
    # 股票代碼
    code = col.StringCol()
    # 年度
    year = col.IntCol()
    # 月
    month = col.IntCol()
    # 法人預估本益比
    per = col.FloatCol()
    # 本益比(季高)
    per_season_high = col.FloatCol()
    # 本益比(季低)
    per_season_low = col.FloatCol()
    # 本益比(近4季)
    per_by_twse = col.FloatCol()


CmoneyPe.createTable(ifNotExists=True)


class BrokerageName(SQLObject):
    """
    券商名稱資訊
    """
    # 券商名稱
    brokerage_name = col.StringCol()


BrokerageName.createTable(ifNotExists=True)


class DayBrokerageActivity(SQLObject):
    """
    個股每日券商買賣超排行
    """
    # 股票代碼
    code = col.StringCol()
    # 日期
    day = col.DateCol(dateFormat='%Y/%m/%d')
    # 券商ID
    brokerage_id = col.IntCol()
    # 合計
    total = col.BigIntCol()
    # 佔成交比重
    percent = col.FloatCol()


DayBrokerageActivity.createTable(ifNotExists=True)


class DayInstitutionalInvestor(SQLObject):
    """
    個股每日法人交易資料
    """
    # 股票代碼
    code = col.StringCol()
    # 日期
    day = col.DateCol(dateFormat='%Y/%m/%d')
    # 法人
    investor = col.StringCol()
    # 合計
    total = col.BigIntCol()


DayInstitutionalInvestor.createTable(ifNotExists=True)

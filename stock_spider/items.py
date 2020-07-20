# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
import datetime
from dataclasses import dataclass


class StockSpiderItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


@dataclass
class StockCodeItem():
    # 有價證券代號
    code: str
    # 有價證券名稱
    name: str
    # 市場別
    marketType: str
    # 有價證券別
    stockType: str
    # 產業別
    industryType: str
    issuanceDate: datetime.datetime

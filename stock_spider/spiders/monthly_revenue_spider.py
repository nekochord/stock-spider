import scrapy
import pandas as pd
from stock_spider.items import RevenueItem


class RevenueSpider(scrapy.Spider):
    name = "monthlyRevenue"

    column_header = [
        # 單月營收
        'sales',
        # 去年同月營收
        'last_year_sales',
        # 單月月增率
        'MoM',
        # 單月年增率
        'YoY',
        # 累計營收
        'accumulated_sales',
        # 去年累計營收
        'last_year_accumulated_sales',
        # 累積年增率
        'accumulated_YoY',
        # 年
        'year',
        # 月
        'month'
    ]

    def __init__(self, *args, **kwargs):
        super(RevenueSpider, self).__init__(*args, **kwargs)

    def start_requests(self):
        urls = ['https://histock.tw/stock/2330/%E6%AF%8F%E6%9C%88%E7%87%9F%E6%94%B6', ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)
        # codes = repository.selectAllStockCode()
        # for stockCode in codes:
        #     url = 'https://histock.tw/stock/{code}/%E6%AF%8F%E6%9C%88%E7%87%9F%E6%94%B6'
        #     print(url)
        #     yield scrapy.Request(
        #         url=url.format(code=stockCode.code),
        #         callback=self.parse,
        #         cb_kwargs={'code': stockCode.code}
        #     )

    def parse(self, response):
        items = []
        item = RevenueItem()

        revenue_df = pd.read_html(response.url)[0]

        spilt_column = revenue_df.columns[0]
        revenue_df['year'], revenue_df['month'] = \
            revenue_df[spilt_column].str.split('/', 1).str
        revenue_df = revenue_df.drop(spilt_column, axis=1)
        revenue_df.columns = self.column_header

        revenue_df = revenue_df.assign(code='2330')
        revenue_df.set_index(keys=["code", 'year', 'month'], inplace=True)

        item['revenue'] = revenue_df
        yield item

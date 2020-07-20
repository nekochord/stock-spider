# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


class StockSpiderPipeline:
    def process_item(self, item, spider):
        return item


class StockCodeItemPipeline:
    # 之後會在這裡寫塞到 DB 的邏輯
    def process_item(self, item, spider):
        return item

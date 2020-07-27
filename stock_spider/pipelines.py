# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import os

from itemadapter import ItemAdapter
from sqlalchemy import create_engine


class StockSpiderPipeline:
    path = 'sqlite:///G:\\SourceCode\\python\\stock-spider\\SQLite\\stock_spider.db'
    engine = create_engine(path, echo=True)
    sqlite_connection = engine.connect()

    def process_item(self, item, spider):
        item['revenue'].to_sql('revenue_test', con=self.engine, if_exists='replace')
        return item

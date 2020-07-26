from stock_spider.entitys import *


def selectAllStockCode():
    '''
    拿到所有 StockCode list
    '''
    return StockCode.select()

from stock_spider.entitys import *


def selectAllStockCode():
    '''
    拿到所有 StockCode list
    '''
    return StockCode.select()


def selectPublicStockCode():
    '''
    拿到所有上市 StockCode list
    '''
    return StockCode.select(StockCode.q.marketType == '上市')

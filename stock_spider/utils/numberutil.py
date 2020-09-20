import re
from decimal import Decimal


def toNumberFormatStr(text):
    return re.sub(r'[^\d.]', '', text)


def toDecimal(text):
    '''把字串轉成 Decimal'''
    numberStr = toNumberFormatStr(text)
    return Decimal(numberStr)


def toInt(text):
    '''把字串轉成 Int'''
    decimal = toDecimal(text)
    return decimal.to_integral_value()


def toFloat(text):
    '''把字串轉成 Float'''
    return float(toDecimal(text))


def toYear(some):
    '''把東西轉成西元年份整數'''
    yearText = str(some)
    match = re.search(r'^[0-9]{4}$', yearText)
    if match != None:
        return toInt(yearText)
    else:
        return None

from re import sub
from decimal import Decimal


def toNumberFormatStr(text):
    return sub(r'[^\d.]', '', text)


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

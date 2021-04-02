import re
from decimal import Decimal, InvalidOperation


def toDecimal(text):
    """把東西轉成 Decimal"""
    numberStr = re.sub(r'[^\d.-]', '', str(text))
    try:
        return Decimal(numberStr)
    except InvalidOperation:
        return None


def toInt(text):
    """把字串轉成 Int"""
    decimal = toDecimal(text)
    if decimal is not None:
        return decimal.to_integral_value()
    else:
        return None


def toFloat(text):
    """把字串轉成 Float"""
    decimal = toDecimal(text)
    if decimal is not None:
        return float(decimal)
    else:
        return None


def toYear(some):
    """把東西轉成西元年份整數"""
    yearText = str(some)
    match = re.search(r'^[0-9]{4}$', yearText)
    if match is not None:
        return toInt(yearText)
    else:
        return None

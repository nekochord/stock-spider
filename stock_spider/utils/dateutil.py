from datetime import *


def dayGenerator(startDate, endDate=date.today()):
    """
    輸入 startDate=(2018,2,18) endDate=(2019,3,18)
    返回日期迭代
    (2018,2,18)
    (2018,2,19)
    ...
    (2019,3,17)
    (2019,3,18)
    """
    dayUnit = timedelta(days=1)
    counter = date(startDate.year, startDate.month, startDate.day)
    while (counter < endDate):
        yield counter
        counter = counter + dayUnit
    yield endDate


def monthGenerator(startDate, endDate=date.today()):
    """
    輸入 startDate=(2018,2,18) endDate=(2019,3,18)
    返回月份迭代
    (2018,2,18)
    (2018,3,1)
    (2018,4,1)
    (2018,5,1)
    ...
    (2019,2,1)
    (2019,3,18)
    """
    dayG = dayGenerator(startDate, endDate)
    month = None

    for day in dayG:
        if (day.month != month):
            month = day.month
            yield day


def yearGenerator(startDate, endDate=date.today()):
    """
    輸入 startDate=(2018,2,18) endDate=(2020,3,18)
    返回年份迭代
    (2018,2,18)
    (2019,1,1)
    (2020,3,18)
    """
    startYear = startDate.year
    endYear = endDate.year
    while (startYear <= endYear):
        yield date(startYear, 1, 1)
        startYear = startYear + 1

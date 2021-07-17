#!/bin/bash

LOG_ROOT_DIR="/Users/redcorvus/Documents/stock_spider/log/"
LOG_DIR="$LOG_ROOT_DIR`date +\%Y-\%m-\%d_%H%M`"

echo "Create log directory $LOG_DIR"

if ! mkdir -p $LOG_DIR; then
	echo "Cannot create $LOG_DIR. Go and fix it!" 1>&2
	exit 1;
fi;


echo "Start to run all_stock_code.py ..."
EACH_LOG="$LOG_DIR/all_stock_code.log"
poetry run scrapy crawl allStockCode>$EACH_LOG 2>&1

echo "Start to run stock_dividend.py ..."
EACH_LOG="$LOG_DIR/stock_dividend.log"
poetry run scrapy crawl stock_dividend>$EACH_LOG 2>&1

echo "Start to run earnings_per_share.py ..."
EACH_LOG="$LOG_DIR/earnings_per_share.log"
poetry run scrapy crawl eps>$EACH_LOG 2>&1

echo "Start to run profit_analysis.py ..."
EACH_LOG="$LOG_DIR/profit_analysis.log"
poetry run scrapy crawl profit_analysis>$EACH_LOG 2>&1

echo "Start to run monthly_revenue.py ..."
EACH_LOG="$LOG_DIR/monthly_revenue.log"
poetry run scrapy crawl monthly_revenue>$EACH_LOG 2>&1

echo "Start to run week_price.py ..."
EACH_LOG="$LOG_DIR/week_price.log"
poetry run scrapy crawl week_price>$EACH_LOG 2>&1
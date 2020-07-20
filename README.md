# stock-spider

## 環境架設

1. 安裝 Python 3.8.X 的版本
2. 安裝 Poetry 
>[官方安裝教學](https://python-poetry.org/docs/#installation)
3. cd 進入 stock_spider 資料夾, 下指令 `poetry install` 會自動安裝依賴
4. 依賴關係的檔案是 `stock_spider/pyproject.toml`

```
[tool.poetry]
name = "stock_spider"
version = "0.1.0"
description = "crawl stock info"
authors = ["Lucas Lin<redcorvus0218@gmail.com>"]

[tool.poetry.dependencies]
//這裡指定要用 3.8 版的 python
python = "^3.8"
//scrapy 的依賴
scrapy = "^2.2.1"
//pandas 的依賴
pandas = "^1.0.5"

[tool.poetry.dev-dependencies]
pytest = "^4.6"

[build-system]
requires = ["poetry>=0.12"]
build-backend = "poetry.masonry.api"
```

## 如何使用 poetry

1. 如何跑 spider

```
// `poetry run` 是讓後面的指令可以跑在專案的環境下， 之後下的 `python XXX` 最好都換成 `poetry run XXX`
// 嫌太長可用 `alias`
poetry run scrapy crawl allStockCode
```

2. 如何加新的依賴

```
poetry add pandas
```

from scrapy.core.downloader.handlers.http11 import HTTP11DownloadHandler


class NoPoolDownloadHandler:
    def __init__(self, settings, crawler=None):
        self._realHandler = HTTP11DownloadHandler(settings, crawler)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings, crawler)

    def download_request(self, request, spider):
        res = self._realHandler.download_request(request, spider)
        return res

    def close(self):
        return self._realHandler.close()

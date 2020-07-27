from scrapy import cmdline


name = 'monthlyRevenue'
cmd = 'scrapy crawl {0}'.format(name)
cmdline.execute(cmd.split())
import scrapy
from scraper_api import ScraperAPIClient
from ..items import PhysempspidersItem as Item
from datetime import datetime
import re

client = ScraperAPIClient('f2a3c4d1c7d60b6d2eb03c55108e3960')

#no businees name listed without profile

class MdsearchSpider(scrapy.Spider):
    name = 'mdsearch'
    custom_settings={ 'FEED_URI': "mdsearch_%(time)s.csv", 'FEED_FORMAT': 'csv'}
    
    def start_requests(self):
        lastpagenum = 2239
        for i in range(lastpagenum):
            next_page = 'https://www.mdsearch.com/physician-jobs/' + str(i)
            yield scrapy.Request(client.scrapyGet(url= next_page), callback=self.parse)

    def parse(self, response):
        for post in response.css('.detailcell'):
            try:
                url = 'https://www.mdsearch.com' + str(response.css('h3 a::attr(href)').get())
                title = response.css('h3 a::text').get()
                date_posted = response.css('#lblDate::text').get().replace('-','').strip()
                location = response.css('.detailcellnormal+ .detailcellattention::text').strip().split(' ')
                business_name = ''
                if(url is not None):
                    yield scrapy.Request(client.scrapyGet(url= url), callback=self.parse_listing, meta={'url': url, 'title': title, 'business_name': business_name})
            except Exception as e:
                print(e)


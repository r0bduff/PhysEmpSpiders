import scrapy
from scraper_api import ScraperAPIClient
from datetime import datetime
import re

client = ScraperAPIClient('f2a3c4d1c7d60b6d2eb03c55108e3960')

class TestSpider(scrapy.Spider):
    name = 'test'

    url_link = 'https://www.hospitalrecruiting.com/jobs/Physician-Jobs/'
    
    start_urls = [client.scrapyGet(url = url_link)]

    custom_settings={ 'FEED_URI': "test_%(time)s.csv", 'FEED_FORMAT': 'csv'}

    def parse(self, response):
        yield {'url': response.css('h2 a::attr(href)').get()}

        next_page = response.css('.next::attr(href)').get()
        if(next_page is not None):
            next_page = 'https://www.hospitalrecruiting.com/jobs/Physician-Jobs' + next_page
            yield scrapy.Request(client.scrapyGet(url= next_page), callback=self.parse)

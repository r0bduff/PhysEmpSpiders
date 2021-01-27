import scrapy


class MedcareersSpider(scrapy.Spider):
    name = 'medcareers'
    allowed_domains = ['website.com']
    start_urls = ['http://website.com/']

    def parse(self, response):
        pass

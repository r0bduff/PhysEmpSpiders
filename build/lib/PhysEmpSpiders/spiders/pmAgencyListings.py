import scrapy
from scraper_api import ScraperAPIClient
client = ScraperAPIClient('f2a3c4d1c7d60b6d2eb03c55108e3960')

class PmagencylistingsSpider(scrapy.Spider):
    name = 'pmAgencyListings'

    start_urls = ['https://www.practicematch.com/employer/AgencyListings.cfm/']

    custom_settings={ 'FEED_URI': "Agency_%(time)s.csv", 'FEED_FORMAT': 'csv'}

    def parse(self, response):
        for emp in response.css('#in-house li'):
            url = 'https://www.practicematch.com' + str(emp.css('a::attr(href)').get())
            name = emp.css('a::text').get()
            yield scrapy.Request(client.scrapyGet(url= url), callback=self.parse_listing, meta={'url': url, 'name': name}, dont_filter=True)

    def parse_listing(self, response):
        if(response.css('.col-sm-3 .recruiter-wrap').get() is not None):
            for rec in response.css('.col-sm-3 .recruiter-wrap'):
                rec_name = rec.css('.nowrapping::text').get()
                rec_phone = rec.css('.phone a::text').get()
                rec_email = rec.css('.contact-info li a::attr(href)').get()
                yield{
                    'url': response.meta['url'],
                    'name': response.meta['name'],
                    'address': response.css('.address-info li::text').get(),
                    'rec_name' : rec_name,
                    'rec_phone' : rec_phone,
                    'rec_email' : rec_email,
                }
        else:
            yield{
                    'url': response.meta['url'],
                    'name': response.meta['name'],
                    'address': response.css('.address-info li::text').get()
            }
        
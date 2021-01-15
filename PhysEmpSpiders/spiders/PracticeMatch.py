import scrapy
from scraper_api import ScraperAPIClient
from ..items import PhysempspidersItem as Item
from datetime import datetime

client = ScraperAPIClient('f2a3c4d1c7d60b6d2eb03c55108e3960')

class PracticematchSpider(scrapy.Spider):
    name = 'PracticeMatch'
    url_link = 'https://www.practicematch.com/physicians/jobs/'
    start_urls = [client.scrapyGet(url = url_link)]

    def parse(self, response):
        for post in response.css('#results > li'):
            url = 'www.practicematch.com' + response.css('.col-md-8 a::attr(href)').get()
            title = response.css('.result-title::text').get()
            business_name = response.css('.results-co::text').get()  
            if(response.css('.result-loc::text').get().find(',') == -1):
               state = response.css('.result-loc::text').get() 
            else:
                city = response.css('.result-loc::text').get().split(',')[0]
                state = response.css('.result-loc::text').get().split(',')[1].strip()
            yield scrapy.Request(client.scrapyGet(url = url), callback=self.parse_listing, meta={'url': url, 'title': title, 'business_name': business_name, 'city': city, 'state': state})
        
        #pagination
        next_page = response.css('li:nth-child(7) span').get()
        if(next_page is not None):
            next_page = 'https://www.practicematch.com' + next_page
            yield scrapy.Request(client.scrapyGet(url= next_page), callback=self.parse)

    def parse_listing(self, response):
        try:
            job = Item({ 
                'title': response.meta['title'],
                'specialty': response.css('div:nth-child(6) td tr:nth-child(1) .info::text').get().replace('\xa0',''),
                'hospital_name': response.css('#job_container div:nth-child(4) td tr:nth-child(1) .info::text').get().replace('\xa0',''),
                'hospital_type': '',
                'job_salary': response.css('tr:nth-child(5) .info::text').get().replace('\xa0',''),
                'job_type': '',
                'job_state': response.meta['state'],
                'job_city': response.meta['city'],
                'job_address': '',
                'date_posted': '',
                'date_scraped': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'source_site': 'PracticeMatch',
                'url': response.meta['url'],
                'description': '',
                'business_type': '',
                'business_name': response.meta['business_name'],
                'contact_name': '',
                'contact_number': '',
                'contact_email': '',
                'business_state': response.meta['state'],
                'business_city': response.meta['city'],
                'business_address': '',
                'business_zip': '',
                'business_website': '',
                'hospital_id': '',
            })
            yield job
        except:
            job = Item({ 
                'title': response.meta['title'],
                'specialty': '',
                'hospital_name': '',
                'hospital_type': '',
                'job_salary': '',
                'job_type': '',
                'job_state': response.meta['state'],
                'job_city': response.meta['city'],
                'job_address': '',
                'date_posted': '',
                'date_scraped': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'source_site': 'PracticeMatch',
                'url': response.meta['url'],
                'description': '',
                'business_type': '',
                'business_name': response.meta['business_name'],
                'contact_name': '',
                'contact_number': '',
                'contact_email': '',
                'business_state': response.meta['state'],
                'business_city': response.meta['city'],
                'business_address': '',
                'business_zip': '',
                'business_website': '',
                'hospital_id': '',
            })
            yield job

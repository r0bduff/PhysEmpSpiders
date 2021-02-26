#Updated to v2.0
import scrapy
from scraper_api import ScraperAPIClient
from ..items import PhysempspidersItem as Item
from datetime import datetime
import re

client = ScraperAPIClient('f2a3c4d1c7d60b6d2eb03c55108e3960')

class PracticematchSpider(scrapy.Spider):
    name = 'PracticeMatch'
    url_link = 'https://www.practicematch.com/physicians/jobs/'
    start_urls = [client.scrapyGet(url = url_link)]

    def parse(self, response):
        for post in response.css('#results > li'):
            url = 'www.practicematch.com' + post.css('.col-md-8 a::attr(href)').get()
            title = post.css('.result-title::text').get()
            business_name = post.css('.results-co::text').get()  
            if(post.css('.result-loc::text').get().find(',') == -1):
               state = post.css('.result-loc::text').get() 
            else:
                city = post.css('.result-loc::text').get().split(',')[0]
                state = post.css('.result-loc::text').get().split(',')[1].strip()
            yield scrapy.Request(client.scrapyGet(url = url), callback=self.parse_listing, meta={'url': url, 'title': title, 'business_name': business_name, 'city': city, 'state': state})
        
        #pagination
        next_page = response.css('li:nth-child(7) span').get()
        if(next_page is not None):
            next_page = 'https://www.practicematch.com' + next_page
            yield scrapy.Request(client.scrapyGet(url= next_page), callback=self.parse)

    def parse_listing(self, response):
        #find emails
        email = ''
        findemail = re.search(r'[\w\.-]+@[\w\.-]+', response.css('.col-sm-8').get())
        if(findemail is not None):
            email = findemail.group(0)

        #find phone numbers
        phone = ''
        findphone = re.search(r'(?:(?:\+?1\s*(?:[.-]\s*)?)?(?:\(\s*([2-9]1[02-9]|[2-9][02-8]1|[2-9][02-8][02-9])\s*\)|([2-9]1[02-9]|[2-9][02-8]1|[2-9][02-8][02-9]))\s*(?:[.-]\s*)?)?([2-9]1[02-9]|[2-9][02-9]1|[2-9][02-9]{2})\s*(?:[.-]\s*)?([0-9]{4})(?:\s*(?:#|x\.?|ext\.?|extension)\s*(\d+))?', response.css('.col-sm-8').get())
        if(findphone is not None):
            phone = findphone.group(0)

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
                'description': str(response.css('.col-sm-8').extract()).replace('<','').replace('>','').replace('"','').replace("[",'').replace("]",'').replace("'",''),
                'business_type': '',
                'business_name': response.meta['business_name'],
                'contact_name': '',
                'contact_number': '',
                'contact_email': '',
                'business_state': '',
                'business_city': '',
                'business_address': '',
                'business_zip': '',
                'business_website': '',
                'hospital_id': '',
                'Ref_num': '',
                'Loc_id': '',
                'Specialty_id': '',
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
                'Ref_num': '',
                'Loc_id': '',
                'Specialty_id': '',
            })
            yield job

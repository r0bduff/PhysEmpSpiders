import scrapy
from scraper_api import ScraperAPIClient
from ..items import PhysempspidersItem as Item
from datetime import datetime
import re

client = ScraperAPIClient('f2a3c4d1c7d60b6d2eb03c55108e3960')

class LocumjobsonlineSpider(scrapy.Spider):
    name = 'locumJobsOnline'

    url_link = 'http://https://www.locumjobsonline.com/jobs/search/'
    
    start_urls = [client.scrapyGet(url = url_link)]

    #custom_settings={ 'FEED_URI': "LocumJobsOnline_%(time)s.csv", 'FEED_FORMAT': 'csv'}

#Parse main page
    def parse(self, response):
        for post in response.css('.sorting_1'):
            try:
                url = post.css('.job_title a::attr(href)').get()
                title = post.css('.job_title a::text').get()
                company = post.css('.mb-2::text').get()
            
                if(url is not None):
                    yield scrapy.Request(client.scrapyGet(url= url), callback=self.parse_listing, meta={'url': url, 'title': title,'company': company})
            except Exception as e:
                print(e)

        #pagination
        next_page = response.css('.pagination li:last-child a::attr(href)').get()
        if(next_page is not None):
            yield scrapy.Request(client.scrapyGet(url= next_page), callback=self.parse)

#Parse listing page
    def parse_listing(self, response):
        print('---------------------CHECKING INTERIOR PAGE--------------------------')

        #find emails
        email = ''
        findemail = re.search(r'[\w\.-]+@[\w\.-]+', response.css('.mb-5~ .mb-5').get())
        if(findemail is not None):
            email = findemail.group(0)

        #find phone numbers
        phone = ''
        findphone = re.search(r'(?:(?:\+?1\s*(?:[.-]\s*)?)?(?:\(\s*([2-9]1[02-9]|[2-9][02-8]1|[2-9][02-8][02-9])\s*\)|([2-9]1[02-9]|[2-9][02-8]1|[2-9][02-8][02-9]))\s*(?:[.-]\s*)?)?([2-9]1[02-9]|[2-9][02-9]1|[2-9][02-9]{2})\s*(?:[.-]\s*)?([0-9]{4})(?:\s*(?:#|x\.?|ext\.?|extension)\s*(\d+))?', response.css('.mb-5~ .mb-5').get())
        if(findphone is not None):
            phone = findphone.group(0)

        location = response.css('.col-xl-6:nth-child(1) .value::text').get().split(',')
        if(len(location) == 2):
            city = location[0]
            state = location[1].strip()
        else:
            state = location[0].strip()
            city = ''
       
        try:
            job = Item({
                'title': response.meta['title'],
                'specialty': response.css('.col-xl-6:nth-child(2) .value::text').get(),
                'hospital_name': '',
                'job_salary': '',
                'job_type': response.css('.col-xl-6:nth-child(4) .value::text').get(),
                'job_state': state,
                'job_city': city,
                'job_address': '',
                'date_posted': '',
                'date_scraped': datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
                'source_site': 'website',
                'url': response.meta['url'],
                'description': '',
                'business_type': '',
                'business_name': response.meta['company'],
                'contact_name': '',
                'contact_number': phone,
                'contact_email': email,
                'business_state': response.css('.address-state::text').get(),
                'business_city': response.css('.address-city::text').get(),
                'business_address': response.css('.address-street::text').get(),
                'business_zip': '',
                'hospital_type': '',
                'business_website': '',
                'hospital_id': '',
            })
            yield job

        except Exception as e:
            job = Item({
                'title': response.meta['title'],
                'specialty': '',
                'hospital_name': '',
                'job_salary': '',
                'job_type': '',
                'job_state': '',
                'job_city': '',
                'job_address': '',
                'date_posted': '',
                'date_scraped': datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
                'source_site': 'website',
                'url': response.meta['url'],
                'description': '',
                'business_type': '',
                'business_name': response.meta['company'],
                'contact_name': '',
                'contact_number': '',
                'contact_email': '',
                'business_state': '',
                'business_city': '',
                'business_address': '',
                'business_zip': '',
                'hospital_type': '',
                'business_website': '',
                'hospital_id': '',
            })
            print(e)
            yield job

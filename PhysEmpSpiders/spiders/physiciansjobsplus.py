import scrapy
from scraper_api import ScraperAPIClient
from ..items import PhysempspidersItem as Item
from datetime import datetime
import re

client = ScraperAPIClient('f2a3c4d1c7d60b6d2eb03c55108e3960')

class PhysiciansjobsplusSpider(scrapy.Spider):
    name = 'physiciansjobsplus'
    #custom_settings={ 'FEED_URI': "template_%(time)s.csv", 'FEED_FORMAT': 'csv'}
    url_link = 'https://www.physiciansjobsplus.com/jobs/browse-category'
    
    start_urls = [client.scrapyGet(url = url_link)]

    def parse(self, response):
        for sp in response.css('#aiAppContainer a::attr(href)').getall():
            try:
                url = 'https://www.physiciansjobsplus.com' + sp
                if(url is not None):
                    yield scrapy.Request(client.scrapyGet(url= url), callback=self.parse_specialty)
            except Exception as e:
                print(str(e))

    def parse_specialty(self, response):
        for post in response.css('.arJobPodMainRow'):
            try:
                url = 'https://www.physiciansjobsplus.com' + str(post.css('.arJobTitle a::attr(href)').get())
                title = post.css('.arJobTitle a::text').get()
                business_name = str(post.css('.arJobCoLink::text').get()).replace('\n','').strip()
                location = str(post.css('.arJobCoLoc::text').get()).strip()
                if(url is not None):
                    yield scrapy.Request(client.scrapyGet(url= url), callback=self.parse_listing, meta={'url': url, 'title': title, 'business_name': business_name, 'location': location})
            except Exception as e:
                print(e)
        
        #pagination
        next_page = response.css('.pagination-next a::attr(href)').get()
        if(next_page is not None):
            yield scrapy.Request(client.scrapyGet(url= next_page), callback=self.parse_specialty)

#Parse listing page
    def parse_listing(self, response):
        print('---------------------CHECKING INTERIOR PAGE--------------------------')

        #find emails
        email = ''
        findemail = re.search(r'[\w\.-]+@[\w\.-]+', response.css('.arDetailDescriptionRow').get())
        if(findemail is not None):
            email = findemail.group(0)

        #find phone numbers
        phone = ''
        findphone = re.search(r'(?:(?:\+?1\s*(?:[.-]\s*)?)?(?:\(\s*([2-9]1[02-9]|[2-9][02-8]1|[2-9][02-8][02-9])\s*\)|([2-9]1[02-9]|[2-9][02-8]1|[2-9][02-8][02-9]))\s*(?:[.-]\s*)?)?([2-9]1[02-9]|[2-9][02-9]1|[2-9][02-9]{2})\s*(?:[.-]\s*)?([0-9]{4})(?:\s*(?:#|x\.?|ext\.?|extension)\s*(\d+))?', response.css('.arDetailDescriptionRow').get())
        if(findphone is not None):
            phone = findphone.group(0)

        location = response.meta['location'].split(',')
        if(len(location) == 2):
            city = location[0]
            state = location[1].strip()
        else:
            state = location[0].strip()
            city = ''
       
        try:
            job = Item({
                'title': response.meta['title'],
                'specialty': '',
                'hospital_name': '',
                'job_salary': '',
                'job_type': '',
                'job_state': state,
                'job_city': city,
                'job_address': '',
                'date_posted': '',
                'date_scraped': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'source_site': 'physiciansjobsplus',
                'url': response.meta['url'],
                'description': '',
                'business_type': '',
                'business_name': response.meta['business_name'],
                'contact_name': '',
                'contact_number': phone,
                'contact_email': email,
                'business_state': state,
                'business_city': city,
                'business_address': '',
                'business_zip': '',
                'hospital_type': '',
                'business_website': '',
                'hospital_id': '',
                'Ref_num': '',
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
                'date_scraped': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'source_site': 'physiciansjobsplus',
                'url': response.meta['url'],
                'description': '',
                'business_type': '',
                'business_name': response.meta['business_name'],
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
                'Ref_num': '',
            })
            print(e)
            yield job

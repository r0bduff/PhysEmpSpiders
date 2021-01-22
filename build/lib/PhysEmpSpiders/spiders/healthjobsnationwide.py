import scrapy
from scraper_api import ScraperAPIClient
from ..items import PhysempspidersItem as Item
from datetime import datetime
import re

client = ScraperAPIClient('f2a3c4d1c7d60b6d2eb03c55108e3960')

class HealthjobsnationwideSpider(scrapy.Spider):
    name = 'healthjobsnationwide'
    #custom_settings={ 'FEED_URI': "hjnationwide_%(time)s.csv", 'FEED_FORMAT': 'csv'}
    #start_urls = ['https://www.healthjobsnationwide.com/jobs/physician?page=1']

    def start_requests(self):
        lastpagenum = 120
        for i in range(lastpagenum):
            next_page = 'https://www.healthjobsnationwide.com/jobs/physician?page=' + str(i)
            yield scrapy.Request(client.scrapyGet(url= next_page), callback=self.parse)


    def parse(self, response):
        for post in response.css('.job__content'):
            try:
                url = post.css('h2 a::attr(href)').get() 
                title = post.css('h2 a::text').get()
                business_name = post.css('.recruiter-company-profile-job-organization::text').get()
                date_posted = post.css('.date::text').get().replace(',','').replace('\n','').strip()
                location = response.css('.location span::text').get()
                if(url is not None):
                    yield scrapy.Request(client.scrapyGet(url= url), callback=self.parse_listing, meta={'url': url, 'title': title, 'business_name': business_name, 'date_posted': date_posted, 'location': location})
            except Exception as e:
                print(e)

#Parse listing page
    def parse_listing(self, response):
        print('---------------------CHECKING INTERIOR PAGE--------------------------')

        #find emails
        email = ''
        findemail = re.search(r'[\w\.-]+@[\w\.-]+', response.css('.even p').get())
        if(findemail is not None):
            email = findemail.group(0)

        #find phone numbers
        phone = ''
        findphone = re.search(r'(?:(?:\+?1\s*(?:[.-]\s*)?)?(?:\(\s*([2-9]1[02-9]|[2-9][02-8]1|[2-9][02-8][02-9])\s*\)|([2-9]1[02-9]|[2-9][02-8]1|[2-9][02-8][02-9]))\s*(?:[.-]\s*)?)?([2-9]1[02-9]|[2-9][02-9]1|[2-9][02-9]{2})\s*(?:[.-]\s*)?([0-9]{4})(?:\s*(?:#|x\.?|ext\.?|extension)\s*(\d+))?', response.css('.even p').get())
        if(findphone is not None):
            phone = findphone.group(0)

        location = response.meta['location']
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
                'date_posted': response.meta['date_posted'],
                'date_scraped': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'source_site': 'healthjobsnationwide',
                'url': response.meta['url'],
                'description': response.css('.even p::text').get(),
                'business_type': '',
                'business_name': response.meta['business_name'],
                'contact_name': '',
                'contact_number': phone,
                'contact_email': email,
                'business_state': '',
                'business_city': '',
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
                'job_state': state,
                'job_city': city,
                'job_address': '',
                'date_posted': '',
                'date_scraped': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'source_site': 'healthjobsnationwide',
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
            print(str(e))
            yield job
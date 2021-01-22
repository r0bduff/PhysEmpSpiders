import scrapy
from scraper_api import ScraperAPIClient
from ..items import PhysempspidersItem as Item
from datetime import datetime
import re

client = ScraperAPIClient('f2a3c4d1c7d60b6d2eb03c55108e3960')

class HospitalrecruitingSpider(scrapy.Spider):
    name = 'hospitalrecruiting'
    
    url_link = 'https://www.hospitalrecruiting.com/jobs/Physician-Jobs/?numberjobs=100&page=1'

    start_urls = [client.scrapyGet(url = url_link)]

    #custom_settings={ 'FEED_URI': "hr_%(time)s.csv", 'FEED_FORMAT': 'csv'}

#Parse main page
    def parse(self, response):
        for post in response.css('.result'):
            try:
                url = post.css('h2 a::attr(href)').get()
                title = post.css('h2 a::text').get()
                business_name = post.css('.location span::text').get()
                if(url is not None):
                    yield scrapy.Request(client.scrapyGet(url= url), callback=self.parse_listing, meta={'url': url, 'title': title, 'business_name': business_name})
            except Exception as e:
                print(e)

        #pagination
        next_page = response.css('.next::attr(href)').get()
        if(next_page is not None):
            next_page = 'https://www.hospitalrecruiting.com/jobs/Physician-Jobs' + next_page
            yield scrapy.Request(client.scrapyGet(url= next_page), callback=self.parse)
            
#Parse listing page
    def parse_listing(self, response):
        print('---------------------CHECKING INTERIOR PAGE--------------------------')

        #find emails
        email = ''
        findemail = re.search(r'[\w\.-]+@[\w\.-]+', response.css('.job_description').get())
        if(findemail is not None):
            email = findemail.group(0)

        #find phone numbers
        phone = ''
        findphone = re.search(r'(?:(?:\+?1\s*(?:[.-]\s*)?)?(?:\(\s*([2-9]1[02-9]|[2-9][02-8]1|[2-9][02-8][02-9])\s*\)|([2-9]1[02-9]|[2-9][02-8]1|[2-9][02-8][02-9]))\s*(?:[.-]\s*)?)?([2-9]1[02-9]|[2-9][02-9]1|[2-9][02-9]{2})\s*(?:[.-]\s*)?([0-9]{4})(?:\s*(?:#|x\.?|ext\.?|extension)\s*(\d+))?', response.css('.job_description').get())
        if(findphone is not None):
            phone = findphone.group(0)

        loc = ["","","",""]

        first = response.css('#job_details_container .value span:nth-child(1)::text').get()
        second = response.css('#job_details_container .value span:nth-child(2)::text').get()
        third = response.css('#job_details_container .value span:nth-child(3)::text').get()
        fourth = response.css('#job_details_container .value span:nth-child(4)::text').get()
        fifth = response.css('#job_details_container .value span:nth-child(5)::text').get()

        if(second is None and third is None):
            loc[1] = first
        elif(second is not None and third is None):
            loc[0] = first
            loc[1] = second
        elif(third is not None and fourth is None):
            loc[0] = first
            loc[1] = second
            loc[2] = third
        elif(fifth is not None):
            loc[0] = third
            loc[1] = fourth
            loc[2] = fifth
            loc[3] = first
        
        business_name = ''
        specialty = ''
        job_type = ''

        for row in response.css('.row'):
            if(row.css('.label::text').get() == 'Company:'):
                business_name = row.css('a::text').get()
            elif(row.css('.label::text').get() == 'Profession/Specialty:'):
                specialty = row.css('a::text').get()
            elif(row.css('.label::text').get() == 'Company'):
                job_type = row.css('.value::text').get()
                
        try:
            job = Item({
                'title': response.meta['title'],
                'specialty': specialty,
                'hospital_name': '',
                'job_salary': '',
                'job_type': job_type,
                'job_state': loc[1],
                'job_city': loc[0],
                'job_address': loc[3],
                'date_posted': '',
                'date_scraped': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'source_site': 'HospitalRecruiting',
                'url': response.meta['url'],
                'description': '',
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
                'source_site': 'HospitalRecruiting',
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
            })
            print(e)
            yield job


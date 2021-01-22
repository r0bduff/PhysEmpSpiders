import scrapy
from scraper_api import ScraperAPIClient
from ..items import PhysempspidersItem as Item
from datetime import datetime
import re

client = ScraperAPIClient('f2a3c4d1c7d60b6d2eb03c55108e3960')

class RadworkingSpider(scrapy.Spider):
    name = 'radworking'

    custom_settings={ 'FEED_URI': "radworking_%(time)s.csv", 'FEED_FORMAT': 'csv'}
    def start_requests(self):
        lastpagenum = 28
        for i in range(lastpagenum):
            next_page = 'http://www.radworking.com/jobs/radiology-jobs.html?page=' + str(i)
            yield scrapy.Request(client.scrapyGet(url= next_page), callback=self.parse)

    def parse(self, response):
        for post in response.css('td a'):
            try:
                url = post.css('a::attr(href)').get() 
                title = post.css('a::text').get()
                if(url is not None):
                    yield scrapy.Request(client.scrapyGet(url= url), callback=self.parse_listing, meta={'url': url, 'title': title})
            except Exception as e:
                print(e)

#Parse listing page
    def parse_listing(self, response):
        print('---------------------CHECKING INTERIOR PAGE--------------------------')

        #find emails
        email = ''
        findemail = re.search(r'[\w\.-]+@[\w\.-]+', response.css('.description_comments').get())
        if(findemail is not None):
            email = findemail.group(0)

        business_type = ''
        bus_name = ''
        yn = str(response.css('#CompanyJobHeader tr:nth-child(3) .data::text').get()).replace('\r','').replace('\t','').replace('\n','').strip()
        if(yn == 'Yes'):
            business_type = 'Agency'
            bus_name = str(response.css('div+ #JobDetail tr:nth-child(3) .data::text').get()).replace('\r','').replace('\t','').replace('\n','').strip()
        else:
            business_type = 'Hospital'
            bus_name = response.css('#JobDetail:nth-child(1) tr:nth-child(1) .data::text').get()

        #find phone numbers
        #phone = ''
        #findphone = re.search(r'(?:(?:\+?1\s*(?:[.-]\s*)?)?(?:\(\s*([2-9]1[02-9]|[2-9][02-8]1|[2-9][02-8][02-9])\s*\)|([2-9]1[02-9]|[2-9][02-8]1|[2-9][02-8][02-9]))\s*(?:[.-]\s*)?)?([2-9]1[02-9]|[2-9][02-9]1|[2-9][02-9]{2})\s*(?:[.-]\s*)?([0-9]{4})(?:\s*(?:#|x\.?|ext\.?|extension)\s*(\d+))?', response.css('.job-description').get())
        #if(findphone is not None):
            #phone = findphone.group(0)

        location = response.css('#JobDetail:nth-child(1) tr:nth-child(2) .data::text').get().split(',')
        if(len(location) == 2):
            city = location[0]
            state = location[1].strip()
        else:
            state = location[0].replace('USA','').strip()
            city = ''
       
        try:
            job = Item({
                'title': response.meta['title'],
                'specialty': str(response.css('.job-right-panel span::text').get()).replace('Subspecialty: ',''),
                'hospital_name': response.css('#JobDetail:nth-child(1) tr:nth-child(1) .data::text').get(),
                'job_salary': '',
                'job_type': '',
                'job_state': state,
                'job_city': city,
                'job_address': '',
                'date_posted': datetime.strptime(response.css('.posted::text').get().replace('Posted: ','') , '%b %d, %Y').strftime('%Y-%m-%d'),
                'date_scraped': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'source_site': 'radworking',
                'url': response.meta['url'],
                'description': str(response.css('.description_comments::text').getall()),
                'business_type': business_type,
                'business_name': bus_name,
                'contact_name': str(response.css('#CompanyJobHeader tr:nth-child(2) .data::text').get()).replace('\r','').replace('\t','').replace('\n',''),
                'contact_number': str(response.css('.c2c-phone::text').get()).replace('\r','').replace('\t','').replace('\n',''),
                'contact_email': email,
                'business_state': '',
                'business_city': '',
                'business_address': '',
                'business_zip': '',
                'hospital_type': '',
                'business_website': '',
                'hospital_id': '',
                'Ref_num': str(response.css('#CompanyJobHeader tr:nth-child(1) .data::text').get()).replace('\r','').replace('\t','').replace('\n',''),
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
                'source_site': 'radworking',
                'url': response.meta['url'],
                'description': '',
                'business_type': '',
                'business_name': 'No Business Name',
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

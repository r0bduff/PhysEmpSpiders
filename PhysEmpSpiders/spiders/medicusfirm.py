#updated to v2.0
import scrapy
from scraper_api import ScraperAPIClient
from ..items import PhysempspidersItem as Item
from datetime import datetime
import re

client = ScraperAPIClient('f2a3c4d1c7d60b6d2eb03c55108e3960')

class MedicusfirmSpider(scrapy.Spider):
    name = 'medicusfirm'

    #custom_settings={ 'FEED_URI': "jamaNetwork_%(time)s.csv", 'FEED_FORMAT': 'csv'}
    def start_requests(self):
        lastpagenum = 45
        for i in range(lastpagenum):
            next_page = 'https://www.themedicusfirm.com/physician/jobs/page=' + str(i)
            yield scrapy.Request(client.scrapyGet(url= next_page), callback=self.parse)

    def parse(self, response):
        for post in response.css('.job-list-item'):
            try:
                url = 'https://www.themedicusfirm.com' + str(post.css('h4 a::attr(href)').get())
                title = post.css('h4 a::text').get() 
                business_name = 'TheMedicusFirm'
                location = post.css('.city_state::text').get() 
                if(url is not None):
                    yield scrapy.Request(client.scrapyGet(url= url), callback=self.parse_listing, meta={'url': url, 'title': title, 'business_name': business_name, 'location': location})
            except Exception as e:
                print(e)

#Parse listing page
    def parse_listing(self, response):
        print('---------------------CHECKING INTERIOR PAGE--------------------------')

        #find emails
        email = ''
        findemail = re.search(r'[\w\.-]+@[\w\.-]+', response.css('.tabs').get())
        if(findemail is not None):
            email = findemail.group(0)

        #find phone numbers
        phone = ''
        findphone = re.search(r'(?:(?:\+?1\s*(?:[.-]\s*)?)?(?:\(\s*([2-9]1[02-9]|[2-9][02-8]1|[2-9][02-8][02-9])\s*\)|([2-9]1[02-9]|[2-9][02-8]1|[2-9][02-8][02-9]))\s*(?:[.-]\s*)?)?([2-9]1[02-9]|[2-9][02-9]1|[2-9][02-9]{2})\s*(?:[.-]\s*)?([0-9]{4})(?:\s*(?:#|x\.?|ext\.?|extension)\s*(\d+))?', response.css('.tabs').get())
        if(findphone is not None):
            phone = '(214) ' + str(findphone.group(0))

        location = str(response.meta['location']).split(',')
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
                'source_site': 'medicusfirm',
                'url': response.meta['url'],
                'description': str(response.css('.col-sm-6').get()).replace('\r','').replace('\t','').replace('\n','').strip(),
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
                'Ref_num': response.css('.standard_list li:nth-child(2)::text').get(),
                'Loc_id': '',
                'Specialty_id': '',
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
                'source_site': 'medicusfirm',
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
                'Loc_id': '',
                'Specialty_id': '',
            })
            print(e)
            yield job
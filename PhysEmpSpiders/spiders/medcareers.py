import scrapy
from scraper_api import ScraperAPIClient
from ..items import PhysempspidersItem as Item
from datetime import datetime
import re

client = ScraperAPIClient('f2a3c4d1c7d60b6d2eb03c55108e3960')

class MedcareersSpider(scrapy.Spider):
    name = 'medcareers'

    #custom_settings={ 'FEED_URI': "template_%(time)s.csv", 'FEED_FORMAT': 'csv'}
    def start_requests(self):
        lastpagenum = 51
        for i in range(lastpagenum):
            next_page = 'https://www.medcareers.com/jobs/?grp=' + str(i)
            yield scrapy.Request(client.scrapyGet(url= next_page), callback=self.parse)

    def parse(self, response):
        for post in response.css('.bodytext table tr:nth-child(2) td:nth-child(1)'):
            try:
                url = post.css('.jobtitle::attr(href)').get() 
                title = str(post.css('.jobtitle strong::text').get()).replace('\r','').replace('\n','')
                business_name = post.css('.jobtitle~ strong::text').get()
                if(url is not None):
                    yield scrapy.Request(client.scrapyGet(url= url), callback=self.parse_listing, meta={'url': url, 'title': title, 'business_name': business_name})
            except Exception as e:
                print(e)
            
#Parse listing page
    def parse_listing(self, response):
        print('---------------------CHECKING INTERIOR PAGE--------------------------')

        #find emails
        email = ''
        findemail = re.search(r'[\w\.-]+@[\w\.-]+', response.css('p , tr:nth-child(7) .bodytexttable').extract())
        if(findemail is not None):
            email = findemail.group(0)

        #find phone numbers
        phone = ''
        findphone = re.search(r'(?:(?:\+?1\s*(?:[.-]\s*)?)?(?:\(\s*([2-9]1[02-9]|[2-9][02-8]1|[2-9][02-8][02-9])\s*\)|([2-9]1[02-9]|[2-9][02-8]1|[2-9][02-8][02-9]))\s*(?:[.-]\s*)?)?([2-9]1[02-9]|[2-9][02-9]1|[2-9][02-9]{2})\s*(?:[.-]\s*)?([0-9]{4})(?:\s*(?:#|x\.?|ext\.?|extension)\s*(\d+))?', response.css('p , tr:nth-child(7) .bodytexttable').extract())
        if(findphone is not None):
            phone = findphone.group(0)

        location = str(response.css('tr:nth-child(6) .bodytexttable::text').get()).split('-')
        if(len(location) == 2):
            city = location[0].strip()
            state = location[1].strip()
        else:
            state = location[0].strip()
            city = ''

        business_name = response.meta['business_name']
        if(business_name == '' or business_name is None):
            business_name = 'no name'

        try:
            job = Item({
                'title': response.meta['title'],
                'specialty': response.css('tr:nth-child(5) .bodytexttable::text').get(),
                'hospital_name': '',
                'job_salary': '',
                'job_type': '',
                'job_state': state,
                'job_city': city,
                'job_address': '',
                'date_posted': datetime.strptime(response.css('.bodytext table tr:nth-child(2) .bodytexttable::text').get(), '%b %d, %Y').strftime('%Y-%m-%d'),
                'date_scraped': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'source_site': 'website',
                'url': response.meta['url'],
                'description': '',
                'business_type': '',
                'business_name': business_name,
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
                'Ref_num': response.css('tr:nth-child(7) .bodytexttable::text').get(),
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
                'source_site': 'website',
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
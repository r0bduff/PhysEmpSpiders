#updated to v2.0
import scrapy
from scraper_api import ScraperAPIClient
from ..items import PhysempspidersItem as Item
from datetime import datetime
import re

client = ScraperAPIClient('f2a3c4d1c7d60b6d2eb03c55108e3960')


class HealthcareercenterSpider(scrapy.Spider):
    name = 'healthcareercenter'
    #custom_settings={ 'FEED_URI': "healthcareercenter_%(time)s.csv", 'FEED_FORMAT': 'csv'}
    url_link = 'https://jobs.healthcareercenter.com/jobs/?discipline=physicians%2Dsurgeons&keywords=&sort=&page=1'
    
    start_urls = [client.scrapyGet(url = url_link)]
    #def start_requests(self):
        #lastpagenum = 102
        #for i in range(lastpagenum):
            #next_page = 'https://jobs.healthcareercenter.com/jobs/?discipline=physicians%2Dsurgeons&keywords=&sort=&page=' + str(i)
            #yield scrapy.Request(client.scrapyGet(url= next_page), callback=self.parse)

    def parse(self, response):
        for post in response.css('.bti-ui-job-detail-container'):
            try:
                url = 'https://jobs.healthcareercenter.com' + str(post.css('.bti-ui-job-result-detail-title a::attr(href)').get())
                title = post.css('.bti-ui-job-result-detail-title a::text').get() 
                business_name = str(post.css('.bti-ui-job-result-detail-employer::text').get()).replace('\t','').replace('\n','').strip()
                if(url is not None):
                    yield scrapy.Request(client.scrapyGet(url= url), callback=self.parse_listing, meta={'url': url, 'title': title, 'business_name': business_name})
            except Exception as e:
                print('-------PAGE NOT FOUND ERROR------' + str(e))

        #pagination
        next_page = response.css('.bti-pagination-prev-next:last-child::attr(href)').get()
        if(next_page is not None):
            next_page = 'https://jobs.healthcareercenter.com' + next_page
            yield scrapy.Request(client.scrapyGet(url= next_page), callback=self.parse)

#Parse listing page
    def parse_listing(self, response):
        print('---------------------CHECKING INTERIOR PAGE--------------------------')

        #find emails
        email = ''
        findemail = re.search(r'[\w\.-]+@[\w\.-]+', response.css('td').get())
        if(findemail is not None):
            email = findemail.group(0)

        #find phone numbers
        phone = ''
        findphone = re.search(r'(?:(?:\+?1\s*(?:[.-]\s*)?)?(?:\(\s*([2-9]1[02-9]|[2-9][02-8]1|[2-9][02-8][02-9])\s*\)|([2-9]1[02-9]|[2-9][02-8]1|[2-9][02-8][02-9]))\s*(?:[.-]\s*)?)?([2-9]1[02-9]|[2-9][02-9]1|[2-9][02-9]{2})\s*(?:[.-]\s*)?([0-9]{4})(?:\s*(?:#|x\.?|ext\.?|extension)\s*(\d+))?', response.css('td').get())
        if(findphone is not None):
            phone = findphone.group(0)

        location = response.css('.bti-jd-detail-text span::text').get().split(',')
        if(len(location) == 2):
            city = location[0]
            state = location[1].strip()
        else:
            state = location[0].strip()
            city = ''
        
        business_name = ''
        #check business name error
        if (response.meta['business_name'] == None or response.meta['business_name'] == ''):
            business_name = response.css('.bti-profile-link::text').get()
        else:
            business_name = response.meta['business_name']
       
        try:
            job = Item({
                'title': response.meta['title'],
                'specialty': response.meta['title'],
                'hospital_name': '',
                'job_salary': response.css('.bti-jd-details-other .bti-jd-detail-text:nth-child(2)::text').get(),
                'job_type': response.css('.bti-jd-details-other .bti-jd-detail-text~ .bti-jd-detail-text::text').get().strip(),
                'job_state': state,
                'job_city': city,
                'job_address': '',
                'date_posted': datetime.strptime(response.css('.bti-jd-details-action .bti-jd-detail-text:nth-child(2)::text').get().strip(), '%B %d, %Y').strftime('%Y-%m-%d'),
                'date_scraped': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'source_site': 'healthcareercenter',
                'url': response.meta['url'],
                'description': response.css('td').get().replace(",",'').replace("'",''),
                'business_type': '',
                'business_name': business_name,
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
                'job_state': '',
                'job_city': '',
                'job_address': '',
                'date_posted': '',
                'date_scraped': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'source_site': 'healthcareercenter',
                'url': response.meta['url'],
                'description': '',
                'business_type': '',
                'business_name': business_name,
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
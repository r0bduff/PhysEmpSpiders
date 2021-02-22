#updated to v2.0
import scrapy
from scraper_api import ScraperAPIClient
from ..items import PhysempspidersItem as Item
from datetime import datetime
import re

client = ScraperAPIClient('f2a3c4d1c7d60b6d2eb03c55108e3960')

class HealthecareersSpider(scrapy.Spider):
    name = 'healthecareers'
    #url_link = 'https://www.healthecareers.com/search-jobs/?catid=&ps=100&pg=1/'
    
    #start_urls = [client.scrapyGet(url = url_link)]

    custom_settings={ 'FEED_URI': "healthecareers_%(time)s.csv", 'FEED_FORMAT': 'csv'}
    
    def start_requests(self):
        lastpagenum = 106
        for i in range(lastpagenum):
            next_page = 'https://www.healthecareers.com/search-jobs/?catid=&ps=100&pg=' + str(i)
            yield scrapy.Request(client.scrapyGet(url= next_page), callback=self.parse)

#Parse main page
    def parse(self, response):
        for post in response.css('.job-results-card'):
            try:
                url = post.css('.job-results-card a::attr(href)').get()  
                title = post.css('#job-results-job-title span::text').get()
                location = post.css('.job-results-card a::attr(data-location)').get() 
                business_name = post.css('.job-results-card #job-results-employer::text').get()
                if(url is not None):
                    yield scrapy.Request(client.scrapyGet(url= url), callback=self.parse_listing, meta={'url': url, 'title': title, 'location': location, 'business_name': business_name})
            except Exception as e:
                print(e)

        #pagination
        #next_page = response.css('#job-results-next a::attr(href)').get() 
        #if(next_page is not None and next_page != 'javascript:void(0);'):
            #yield scrapy.Request(client.scrapyGet(url= next_page), callback=self.parse)

#Parse listing page
    def parse_listing(self, response):
        print('---------------------CHECKING INTERIOR PAGE--------------------------')

        #find emails
        email = ''
        findemail = re.search(r'[\w\.-]+@[\w\.-]+', response.css('.job-description').get())
        if(findemail is not None):
            email = findemail.group(0)

        #find phone numbers
        phone = ''
        findphone = re.search(r'(?:(?:\+?1\s*(?:[.-]\s*)?)?(?:\(\s*([2-9]1[02-9]|[2-9][02-8]1|[2-9][02-8][02-9])\s*\)|([2-9]1[02-9]|[2-9][02-8]1|[2-9][02-8][02-9]))\s*(?:[.-]\s*)?)?([2-9]1[02-9]|[2-9][02-9]1|[2-9][02-9]{2})\s*(?:[.-]\s*)?([0-9]{4})(?:\s*(?:#|x\.?|ext\.?|extension)\s*(\d+))?', response.css('.job-description').get())
        if(findphone is not None):
            phone = findphone.group(0)

        location = response.meta['location']
        comma = ','
        if comma in location:
            location = location.split(',')
            city = location[0]
            state = location[1].strip()
        else:
            state = location
            city = ''
        
        jobtype = response.css('#tag-26::text').get()
        if(jobtype is None):
            jobtype = response.css('#tag-25::text').get()
            if(jobtype is None):
                jobtype = ''
       
        try:
            job = Item({
                'title': response.meta['title'],
                'specialty': response.meta['title'],
                'hospital_name': '',
                'job_salary': '',
                'job_type': jobtype,
                'job_state': state,
                'job_city': city,
                'job_address': '',
                'date_posted': response.css('li:nth-child(1) p::text').get().replace('Date Posted: ',''),
                'date_scraped': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'source_site': 'healthecareers',
                'url': response.meta['url'],
                'description': response.css('.job-description').get().replace(",",'').replace("'",''),
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
                'Ref_num': response.css('li+ li p::text').get().replace('Job Id: ', ''),
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
                'source_site': 'healthecareers',
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
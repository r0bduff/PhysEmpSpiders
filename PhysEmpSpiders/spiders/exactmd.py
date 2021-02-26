#updated to v2.0
import scrapy
from scraper_api import ScraperAPIClient
from ..items import PhysempspidersItem as Item
from datetime import datetime
import re

client = ScraperAPIClient('f2a3c4d1c7d60b6d2eb03c55108e3960')

class ExactmdSpider(scrapy.Spider):
    name = 'exactmdspider'

    #url_link = 'http://exactmd.com/jobs/all/all/all/all?page=344&sort=date_desc'
    
    #start_urls = [client.scrapyGet(url = url_link)]

    #custom_settings={ 'FEED_URI': "data/exactmd_%(time)s.csv", 'FEED_FORMAT': 'csv'}

    def start_requests(self):
        lastpagenum = 1837
        for i in range(lastpagenum):
            next_page = 'https://exactmd.com/jobs/all/all/all/all?page=' + str(i) + '&sort=date_desc'
            yield scrapy.Request(client.scrapyGet(url= next_page), callback=self.parse)
    #check main search page with all the listings
    def parse(self, response):
        for post in response.css('.card'):
            url = 'http://exactmd.com' + post.css('.card-title::attr(href)').get()
            specialty = post.css('.card-title::text').get()
            business_type = post.css('.text-sm-right::text').get().replace('\n','') 
            salary = post.css('strong::text').get()
            location = post.css('em::text').get().replace('\n','')
            yield scrapy.Request(client.scrapyGet(url= url), callback=self.parse_listing, meta={'url': url, 'specialty': specialty, 'business_type': business_type, 'salary': salary, 'location': location})

        #pagination
        #next_page = 'http://exactmd.com' + response.css('.pagination li:last-child a::attr(href)').get()
        #if next_page is not None:   
            #yield scrapy.Request(client.scrapyGet(url= next_page), callback=self.parse)

    #checks interior page of the individual listing then yields results
    def parse_listing(self, response):
        print('---------------------CHECKING INTERIOR PAGE--------------------------')
        
        #find emails
        email = ''
        findemail = re.search(r'[\w\.-]+@[\w\.-]+', response.css('.details').get())
        if(findemail is not None):
            email = findemail.group(0)

        #find phone numbers
        phone = ''
        ref = ''
        findphone = re.search(r'(?:(?:\+?1\s*(?:[.-]\s*)?)?(?:\(\s*([2-9]1[02-9]|[2-9][02-8]1|[2-9][02-8][02-9])\s*\)|([2-9]1[02-9]|[2-9][02-8]1|[2-9][02-8][02-9]))\s*(?:[.-]\s*)?)?([2-9]1[02-9]|[2-9][02-9]1|[2-9][02-9]{2})\s*(?:[.-]\s*)?([0-9]{4})(?:\s*(?:#|x\.?|ext\.?|extension)\s*(\d+))?', response.css('.details').get())
        if(findphone is not None):
            phone = findphone.group(0)
            if(len(phone) == 7):
                ref = phone
                phone = ''

        location = str(response.meta['location']).split(',')
        if(len(location) == 2):
            city = location[0]
            state = location[1].strip()
        else:
            state = location[0].strip()
            city = ''

        try:
            job = Item({
                'title': response.css('.job-detail-title::text').get(),
                'specialty': response.meta['specialty'],
                'hospital_name': '',
                'job_salary': response.meta['salary'],
                'job_type': response.css('li:nth-child(1) .job-detail-label-primary+ span::text').get(),
                'job_state': state,
                'job_city': city,
                'job_address': '',
                'date_posted': datetime.strptime(response.xpath("//meta[@itemprop='datePosted']/@content")[0].extract().replace('T', ' ').replace('+',' +'), '%Y-%m-%d %H:%M:%S %z').strftime('%Y-%m-%d'),
                'date_scraped': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'source_site': 'exactmd',
                'url': response.meta['url'],
                'description': str(response.css('.details').get()),
                'business_type': response.meta['business_type'],
                'business_name': response.css('.col-md-6 .job-detail-label+ span::text').get(),
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
                'Loc_id': '',
                'Specialty_id': '',
            })
            yield job
        
        except Exception as e:
            job = Item({
                'title': '',
                'specialty': response.meta['specialty'],
                'hospital_name': '',
                'job_salary': response.meta['salary'],
                'job_type': '',
                'job_state': response.meta['location'],
                'job_city': '',
                'job_address': '',
                'date_posted': '',
                'date_scraped': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'source_site': 'exactmd',
                'url': response.meta['url'],
                'description': '',
                'business_type': response.meta['business_type'],
                'business_name': 'business-name',
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
            print(str(e))
            yield job
    

            

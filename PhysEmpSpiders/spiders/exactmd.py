"""
@name:exactmdspider.py
@author: Rob Duff
@description:Spider webcrawler for exactmd.com
@not operational
"""
import scrapy
from scraper_api import ScraperAPIClient
from ..items import PhysempspidersItem as Item
from datetime import datetime

client = ScraperAPIClient('f2a3c4d1c7d60b6d2eb03c55108e3960')

class ExactmdSpider(scrapy.Spider):
    name = 'exactmdspider'

    url_link = 'http://exactmd.com/jobs/all/all/all/all?page=344&sort=date_desc'
    
    start_urls = [client.scrapyGet(url = url_link)]

    #custom_settings={ 'FEED_URI': "exactmd_%(time)s.csv", 'FEED_FORMAT': 'csv'}

    #check main search page with all the listings
    def parse(self, response):
        for post in response.css('.card'):
            url = 'http://exactmd.com' + post.css('.card-title::attr(href)').get()
            specialty = post.css('.card-title::text').get()
            employer = post.css('.col-sm-4 div+ div::text').get().replace('\n','')
            employment = post.css('.text-sm-right::text').get().replace('\n','') 
            salary = post.css('strong::text').get()
            location = post.css('em::text').get().replace('\n','')
            yield scrapy.Request(client.scrapyGet(url= url), callback=self.parse_listing, meta={'url': url, 'specialty': specialty, 'employer': employer, 'employment': employment, 'salary': salary, 'location': location})

        #pagination
        next_page = 'http://exactmd.com' + response.css('.pagination li:last-child a::attr(href)').get()
        if next_page is not None:   
            yield scrapy.Request(client.scrapyGet(url= next_page), callback=self.parse)

    #checks interior page of the individual listing then yields results
    def parse_listing(self, response):
        print('---------------------CHECKING INTERIOR PAGE--------------------------')
        
        try:
            job = Item({
                'title': response.css('.job-detail-title::text').get(),
                'specialty': response.meta['specialty'],
                'hospital_name': response.meta['employer'],
                'job_salary': response.meta['salary'],
                'job_type': response.css('li:nth-child(1) .job-detail-label-primary+ span::text').get(),
                'job_state': response.meta['location'],
                'job_city': '',
                'job_address': '',
                'date_posted': response.xpath("//meta[@itemprop='datePosted']/@content")[0].extract().replace('T', ' ').replace('+',' +'),
                'date_scraped': datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
                'source_site': 'www.exactmd.com',
                'url': response.meta['url'],
                'description': response.css('.MsoNormal::text').get(),
                'business_type': response.meta['employment'],
                'business_name': response.css('.col-md-6 .job-detail-label+ span::text').get(),
                'contact_name': '',
                'contact_number': '',
                'contact_email': '',
                'business_state': '',
                'business_city': '',
                'business_address': '',
                'business_zip': '',
            })
            yield job
        
        except:
            job = Item({
                'title': '',
                'specialty': response.meta['specialty'],
                'hospital_name': response.meta['employer'],
                'job_salary': response.meta['salary'],
                'job_type': '',
                'job_state': response.meta['location'],
                'job_city': '',
                'job_address': '',
                'date_posted': '',
                'date_scraped': datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
                'source_site': 'www.exactmd.com',
                'url': response.meta['url'],
                'description': '',
                'business_type': response.meta['employment'],
                'business_name': '',
                'contact_name': '',
                'contact_number': '',
                'contact_email': '',
                'business_state': '',
                'business_city': '',
                'business_address': '',
                'business_zip': '',
            })
            yield job
    

            

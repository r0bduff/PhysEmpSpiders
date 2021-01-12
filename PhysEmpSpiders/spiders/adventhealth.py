import scrapy
from scraper_api import ScraperAPIClient
from ..items import PhysempspidersItem as Item
from datetime import datetime
from scrapy.exceptions import DontCloseSpider

client = ScraperAPIClient('f2a3c4d1c7d60b6d2eb03c55108e3960')

class AdventhealthSpider(scrapy.Spider):
    name = 'adventhealth'
    
    #custom_settings={ 'FEED_URI': "AdventHealth_%(time)s.csv", 'FEED_FORMAT': 'csv'}

    url_link = 'https://jobs.adventhealth.com/en-US/search?pagenumber=1'
    
    start_urls = [client.scrapyGet(url = url_link)]

    def parse(self, response):
        for post in response.css('.job-result'):
            url = 'jobs.adventhealth.com' + post.css('.job-result-title-cell a::attr(href)').get()
            title = post.css('.job-result-title-cell a::text').get()
            business_name = post.css('.job-result-company-cell::text').get().replace('\n', '').strip()
            city = post.css('.job-location-line::text').get().split(',')[0]
            state = post.css('.job-location-line::text').get().split(',')[1].strip()
            date_posted = post.css('.job-result-date-posted-cell::text').get().replace('\n', '').strip()
            yield scrapy.Request(client.scrapyGet(url = url), callback=self.parse_listing, meta={'url': url, 'title': title, 'business_name': business_name, 'date_posted': date_posted, 'city': city, 'state': state})

        next_page = response.css('.next-page-caret::attr(href)').get()
        if(next_page is not None):   
            next_page = 'https://jobs.adventhealth.com' + next_page
            yield scrapy.Request(client.scrapyGet(url= next_page), callback=self.parse)

    def parse_listing(self, response):
        print('---------------------CHECKING INTERIOR PAGE--------------------------')
        
        try:
            job = Item({ 
                'title': response.meta['title'],
                'specialty': response.css('.discrete-field-3 .snapshot-text .secondary-text-color::text').get(),
                'hospital_name': response.meta['business_name'],
                'hospital_type': '',
                'job_salary': '',
                'job_type': response.css('.job-employee-type .snapshot-text .secondary-text-color::text').get(),
                'job_state': response.meta['state'],
                'job_city': response.meta['city'],
                'job_address': '', #re.findall("(?<=Address:)(.*)(?=Top Reasons To Work)",response.css('#jdp-job-description-section :nth-child(1)::text').get())[0].replace('</b>\xa0210','').replace('</p> <p><b>',''),
                'date_posted': response.meta['date_posted'],
                'date_scraped': datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
                'source_site': 'adventhealth',
                'url': response.meta['url'],
                'description': '',#response.css('#jdp-job-description-section :nth-child(1)::text').extract(),
                'business_type': 'Hospital',
                'business_name': response.meta['business_name'],
                'contact_name': '',
                'contact_number': '',
                'contact_email': '',
                'business_state': response.meta['state'],
                'business_city': response.meta['city'],
                'business_address': '',
                'business_zip': '',
                'business_website': '',
                'hospital_id': '',
            })
            yield job

        except:
            job = Item({ 
                'title': response.meta['title'],
                'specialty': '',
                'hospital_name': response.meta['business_name'],
                'hospital_type': '',
                'job_salary': '',
                'job_type': '',
                'job_state': response.meta['state'],
                'job_city': response.meta['city'],
                'job_address': '',
                'date_posted': response.meta['date_posted'],
                'date_scraped': datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
                'source_site': 'adventhealth',
                'url': response.meta['url'],
                'description': '',
                'business_type': 'Hospital',
                'business_name': response.meta['business_name'],
                'contact_name': '',
                'contact_number': '',
                'contact_email': '',
                'business_state': response.meta['state'],
                'business_city': response.meta['city'],
                'business_address': '',
                'business_zip': '',
                'business_website': '',
                'hospital_id': '',
            })
            yield job


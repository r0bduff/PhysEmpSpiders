#Updated to match v2.0 of scraper 2-22-2021
import scrapy
from scraper_api import ScraperAPIClient
from ..items import PhysempspidersItem as Item
from datetime import datetime
import re

client = ScraperAPIClient('f2a3c4d1c7d60b6d2eb03c55108e3960')

class NejmcareercenterSpider(scrapy.Spider):
    name = 'nejmcareercenter'

    #url_link = 'https://www.nejmcareercenter.org/jobs/1/'
    
    #start_urls = [client.scrapyGet(url = url_link)]

    #custom_settings={ 'FEED_URI': "jamaNetwork_%(time)s.csv", 'FEED_FORMAT': 'csv'}

    def start_requests(self):
        lastpagenum = 429
        for i in range(lastpagenum):
            next_page = 'https://www.nejmcareercenter.org/jobs/' + str(i)
            yield scrapy.Request(client.scrapyGet(url= next_page), callback=self.parse)

#Parse main page
    def parse(self, response):
        for post in response.css('.lister__item'):
            try:
                url = 'https://www.nejmcareercenter.org' + str(post.css('.lister__header a::attr(href)').get()).replace('\r','').replace('\t','').replace('\n','').strip()
                title = post.css('.js-clickable-area-link span::text').get().replace('\xa0',' ')
                location = post.css('.lister__meta-item--location::text').get()
                business_name = post.css('.lister__meta-item--recruiter::text').get()
                if(url is not None):
                    yield scrapy.Request(client.scrapyGet(url= url), callback=self.parse_listing, meta={'url': url, 'title': title, 'location': location, 'business_name': business_name})
            except Exception as e:
                print(e)

        #pagination
        #next_page = response.css('.paginator__items li:nth-child(7) a::attr(href)').get()
        #if(next_page is not None):
            #next_page = 'https://www.nejmcareercenter.org' + next_page
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
       
        try:
            job = Item({
                'title': response.meta['title'],
                'specialty': response.css('.job-detail-description__category-Specialty dd a::text').get(),
                'hospital_name': '',
                'job_salary': response.css('.job-detail-description__salary dd span::text').get(),
                'job_type': response.css('.job-detail-description__category-PositionType dd a::text').get(),
                'job_state': state,
                'job_city': city,
                'job_address': '',
                'date_posted': datetime.strptime(response.css('.job-detail-description__posted-date dd span::text').get(), '%b %d, %Y').strftime('%Y-%m-%d'),
                'date_scraped': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'source_site': 'nejmcareercenter',
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
                'source_site': 'nejmcareercenter',
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

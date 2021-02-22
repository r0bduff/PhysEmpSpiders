#updated to v2.0
import scrapy
from scraper_api import ScraperAPIClient
from ..items import PhysempspidersItem as Item
from datetime import datetime

client = ScraperAPIClient('f2a3c4d1c7d60b6d2eb03c55108e3960')

class DoccafeSpider(scrapy.Spider):
    name = 'doccafe'

    url_link = 'https://www.doccafe.com/jobs/search?posted_period=all&results_per_page=50&page=682'

    start_urls = [client.scrapyGet(url = url_link)]

    #custom_settings={ 'FEED_URI': "DocCafe_%(time)s.csv", 'FEED_FORMAT': 'csv'}kjdf

    def parse(self, response):
        for post in response.css('div.job'):   
            state = post.css('.dashboard-action-icon-color+ .dashboard-action-icon-color::text').get()
            city = post.css('.fa-map-marker+ .dashboard-action-icon-color::text').get()
            if(state is None):
                state = city
                city = ''

            job = Item({
                'title': str(post.css('.col-xs-12 .h5 strong::text').get()).strip(),
                'specialty': post.css('a.text-black+ .text-black::text').get() + ' ' + post.css('a+ .text-black::text').get(),
                'hospital_name': post.css('.text-black .text-black::text').get(),
                'hospital_type': '',
                'job_salary': '',
                'job_type': '',
                'job_state': state,
                'job_city': city,
                'job_address': '', 
                'date_posted': datetime.strptime(response.css('.field-row+ .field-row small').get().replace('<small>\n    <i class="fa fa-calendar"></i>\xa0\n    ','').replace('\n</small>',''), '%b %d, %Y').strftime('%Y-%m-%d'),
                'date_scraped': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'source_site': 'doccafe',
                'url': 'www.doccafe.com' + str(post.css('.h5::attr(href)').get()),
                'description': '',
                'business_type': '',
                'business_name': post.css('.text-black .text-black::text').get(),
                'contact_name': '',
                'contact_number': '',
                'contact_email': '',
                'business_state': '',
                'business_city': '',
                'business_address': '',
                'business_zip': '',
                'business_website': '',
                'hospital_id': '',
                'Loc_id': '',
                'Specialty_id': '',
            })
            yield job
        
        next_page = response.css('.pagination li:last-child a::attr(href)').get()
        if next_page is not None:
            next_page = 'www.doccafe.com' + next_page
            yield scrapy.Request(client.scrapyGet(url= next_page), callback=self.parse)
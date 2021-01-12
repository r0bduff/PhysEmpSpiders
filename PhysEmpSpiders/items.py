# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class PhysempspidersItem(scrapy.Item):
    title = scrapy.Field()
    specialty = scrapy.Field()
    hospital_type = scrapy.Field()
    hospital_name = scrapy.Field()
    job_salary = scrapy.Field()
    job_type = scrapy.Field()
    job_state = scrapy.Field()
    job_city = scrapy.Field()
    job_address = scrapy.Field()
    date_posted = scrapy.Field()
    date_scraped = scrapy.Field()
    source_site = scrapy.Field()
    url = scrapy.Field()
    description = scrapy.Field()
    business_type = scrapy.Field()
    business_name = scrapy.Field()
    contact_name = scrapy.Field()
    contact_number = scrapy.Field()
    contact_email = scrapy.Field()
    business_state = scrapy.Field()
    business_city = scrapy.Field()
    business_address = scrapy.Field()
    business_zip = scrapy.Field()
    business_website = scrapy.Field()
    hospital_id = scrapy.Field()

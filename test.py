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
       
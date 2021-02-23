"""
@name: piplines.py
@author: Rob Duff
@description: Takes items object created from each scraped page. Sends it to the database for storage.
"""
# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from PhysEmpSpiders.services.routes import Routes
from PhysEmpSpiders.services.packages import *
import nltk
from nltk.corpus import stopwords
import pymssql

#https://stackoverflow.com/questions/43266482/scrapy-how-to-crawl-website-store-data-in-microsoft-sql-server-database/43266807


#Class PysEmpPipeline

class PhysempspidersPipeline:
    #set up a new route to the db containing all sql queries
    def __init__(self):
        self.route = Routes()
    #Called whenever an item object is yeilded. Handles all logic for sending data to the DB
    def process_item(self, item, spider):
        #Gives empty values a null value so they can go into the db in a nice way. Storing as '' is no bueno.
        for i in item:
            if(item[i] == ''):
                item[i] = None
        #check if the url exists in the db, if not then we will add it to the db
        if(self.Check_url(item['url'])==False):
            E_id = None
            #get the recruiter information matching the business name found
            row = self.route.spSelectBusinessName(item['business_name'])
            #format state name
            if(item['job_state'] is not None):
                item['job_state'] = self.Get_Abbrev(str(item['job_state']))
                #get location id if city AND state not null
                if(item['job_city'] is not None):
                    item['Loc_id'] = self.Get_Loc(item)
            #fix email
            if item['contact_email'] is not None:
                item['contact_email'] = str(item['contact_email']).strip('.')
            #The above try returns a single row from the DB of the matching recruiter id
            #If that row exists then we will insert a new job.
            if(row is not None):
                #update recruiter if email or number exists - add emp runs from this
                if(item['contact_email'] is not None or item['contact_number'] is not None):
                    E_id = self.Update_Recruiter(row, item)
                #add job with recruiter_id inserted into the job. row containts recruiter id
                #check if hospital info lines up with db entry. Update job info if necessary.
                item = self.Check_Hospital(item)                    
                self.Insert_Job(row[0], item, E_id)
            #If there is no row then a new recruiter is added to the database. 
            else:
                #make new recruiter
                id = self.Insert_Recruiter(item)
                #make new job with new recruiter
                #check if hospital info lines up with db entry. Update job info if necessary.
                item = self.Check_Hospital(item)
                self.Insert_Job(id, item, E_id)
        else:
            print('Updating Job')
            item['job_state'] = self.Get_Abbrev(str(item['job_state']))
            self.Update_Job(item)
        #Item is returned at the end dont ask me why 
        return item

#@method:Insert_Job
#@description: inserts a new job into the database. Must be given a recruiter id as an integer
    def Insert_Job(self, R_id, item, Emp_Id):
        item['Specialty_id'] = self.Check_Specialty(item)
        #Insert new job
        self.route.spInsertJob(R_id, Emp_Id, item['title'], item['hospital_type'], item['job_salary'], item['job_type'], item['job_state'], item['job_city'], item['job_address'], item['source_site'], item['url'], item['description'],item['hospital_id'], item['hospital_name'], item['Ref_num'], item['Loc_id'],item['specialty'], item['Specialty_id'])
        #add time item to record last scrape
        self.Insert_Time(item)

#@method: Insert_Time
#@description: adds a time row to Job_Time_Item to track when an ad was scraped.
    def Insert_Time(self, item): 
        Job_id = self.route.spSelectJobIdURL(item['url'])
        if Job_id is not None:
            self.route.spInsertJobTimeItem(Job_id[0], item["date_scraped"], item["date_posted"]) 
        else:
            print("Error 1 - Job not found in Insert Time")

#@method: Update_Job
#@description: updates Job if url already exists.
    def Update_Job(self, item):
        item['Specialty_id'] = self.Check_Specialty(item)
        #updates job
        self.route.spUpdateJobNull(item['title'], item['hospital_type'], item['job_salary'], item['job_type'], item['job_state'], item['job_city'], item['job_address'], item['source_site'], item['url'], item['description'],item['hospital_id'], item['hospital_name'], item['Ref_num'], item['Loc_id'],item['specialty'], item['Specialty_id'])
        #add time item to record last scrape
        self.Insert_Time(item)

#@method: Update_Job_R
#@description: updates by replacing all data in the row matching a url
    def Update_Job_R(self, item):
        #finds a specialty Id for the given specialty
        item['Specialty_id'] = self.Check_Specialty(item)
        #updates job
        self.route.spUpdateJobAll(item['title'], item['hospital_type'], item['job_salary'], item['job_type'], item['job_state'], item['job_city'], item['job_address'], item['source_site'], item['url'], item['description'],item['hospital_id'], item['hospital_name'], item['Ref_num'], item['Loc_id'],item['specialty'], item['Specialty_id'])
        #add time item to record last scrape
        self.Insert_Time(item)

#@method: Insert_Recruiter
#@dexcription: inserts a new recruiter into the database.
    def Insert_Recruiter(self, item):
        match = self.route.spSelectHospitalName(item['business_name'], item['business_city'], item['business_state']) #finds if the business is a hospital
        if match is not None:
            item['business_type'] = 'hospital'
            item['hospital_id'] = match[0]
            item['business_address'] = match[1]
        #make new recruiter
        self.route.spInsertRecruiter(item['business_type'], item['business_name'], item['contact_name'], item['contact_number'], item['contact_email'], item['business_state'], item['business_city'], item['business_address'], item['business_zip'], item['business_website'], item['hospital_id'])
        #return new Recruiter ID
        id = self.route.spSelectNewRecruiter()[0]
        return id

#@method: Update_Recruiter
#@description: When given a recruiter id (R_id) it pulls the row and attempts to fill all empty columns. Then updates the row with the data.
    def Update_Recruiter(self, Recruiter, item):
        #if the recruiter does exist then fill empty fields with new info. Save old info
        E_id = None
        if(Recruiter is not None):   
            match = self.route.spSelectHospitalName(item['business_name'], item['business_city'], item['business_state']) #finds if the business is a hospital
            if match is not None:
                item['business_type'] = 'hospital'
                item['hospital_id'] = match[0]
                item['business_address'] = match[1]
            #update the recruiter
            self.route.spUpdateRecruiter(item['business_type'], item['business_name'], item['contact_name'], item['contact_number'], item['contact_email'], item['business_state'], item['business_city'], item['business_address'], item['business_zip'], item['business_website'], item['hospital_id'], Recruiter[0])
            if(item['contact_number'] is not None):
                if(Recruiter[4] is not None and Recruiter[4] != item['contact_number']):
                    E_id = self.Insert_Emp(Recruiter, item)
            if(item['contact_email'] is not None):
                if(Recruiter[5] is not None and Recruiter[5] != item['contact_email']):
                    E_id = self.Insert_Emp(Recruiter, item)
        return E_id

#@method: Check_url
#@description: checks to verify that the url of a job listing does not already exist in the db.
#              This eliminates duplicates at the cost of potential preformance in the long run?    
    def Check_url(self, url):
        test = self.route.spSelectJobURL(url)
        #print(test)
        if(test is not None):
            return True
        else:
            return False

#@method: Check_Hospital - Decommissioned for now
#@description: checks to see if the hospital name matches the name found(not super common but possible if a secretary spells right)
#              returns an ID, ADDRESS, CITY, STATE to associate with the job. Now that I think about it this is double storing data, but is needed for any job not hospital based.
    def Check_Hospital(self, item):
        hospital = None

        if(item['hospital_name'] is not None and hospital is None):
            hospital = self.route.spSelectHospitalName(item['hospital_name'], item['job_city'], item['job_state'])
            #print('ran 1' + str(hospital))
        
        if(hospital is None):
            #set up stopwords
            more_stopwords = set(())
            stoplist = set(stopwords.words('english')) | more_stopwords
            hospitals = None
            #select hospitals in the same location as the job
            if item['Loc_id'] is not None:
                hospitals = self.route.spSelectHospitalLocation(item["job_city"], item["job_state"])
                #print('ran 2' + str(hospitals))

            #of those hospitals lets try to narrow down the right one from the description
            found = [] #to hold the best matching
            desc = str(item['description']).lower().replace("’","").replace("'","").replace(">"," ").replace("<", " ")

            if hospitals is not None:
                for h in hospitals:
                    #print("H equals: " + str(h))
                    name = str(h[0]).lower() #lowercase and save hospitalname
                    testArr = name.replace("'",'').split(' ') #array of strings 
                    testArr.append(name) #add the name to the end of test array
                    b = True #test variable b (bool)
                    cnt = 0 #counts total of all ids
                    for t in testArr:
                        if t not in stoplist:
                            t = " " + t + " "
                            ids = []
                            #print(t)
                            ids.append(desc.count(t))
                            cnt += desc.count(t)
                            for i in ids:
                                #print(i)
                                if t.strip() == testArr[len(testArr) - 1]:
                                    if i != 0:
                                        cnt += 1000
                                        #print(cnt)
                                else:
                                    if i == 0:
                                        b = False #id does not exist
                    if b == True:
                        found.append([name, cnt, h[4]])
                #for loop ends
                #check how many results were found
                out = ["", 0, None]
                #if one item has been found then that has to be it
                if len(found) == 1:
                    for f in found:
                        out[0] = f[0]
                        out[1] = f[1]
                        out[2] = f[2]       
                #if there are more than one then lets see which has a higher cnt         
                elif len(found) > 1: 
                    for f in found:
                        if f[1] > out[1]:
                            out[0] = f[0]
                            out[1] = f[1]
                            out[2] = f[2]
                #if we did find something then lets grab that hospitals info.
                if out[2] is not None:
                    hospital = self.route.spSelectHospitalID(out[2])[0]
        #return item variable at the end, updated or not
        #print("HOSPITAL EQUALS: " + str(hospital))
        if hospital is not None:
            if(item['job_state'] == None): item['job_state'] = hospital[2]
            if(item['job_city'] == None): item['job_city'] = hospital[1]
            if(item['job_address'] == None): item['job_address'] = hospital[3]
            item['hospital_name'] = hospital[0]
            item['hospital_id'] = hospital[4]
        return item

#@method Check_Specialty
#@description: Returns a Specialty Id that matches the given Specialty. Returns Null if no Specialty is given
    def Check_Specialty(self, item):
        Specialty_Id = None #default return value
        if item['specialty'] is not None:
            match = self.route.spSelectSpecialty(item['specialty']) #returns exact matches to the given specialty
            matching = []
            if match is not None:
                Specialty_Id = match[0]
            else:
                words = str(item['specialty']).replace(":",'').replace(',','').replace("'","").replace("/"," ").strip().split(' ')
                for w in words:
                    matches = self.route.spSelectSpecialtyLike(w) #returns any similar matches to the first word in the specialty
                    if matches is not None:
                        for i in matches:
                            matching.append(i)
                found = []
                if matching is not None:
                    #for each matching specialty given them a score based on how many word match our specialty
                    for m in matching:
                        cnt = 0
                        for w in words:
                            if w in m[1]:
                                cnt += 1
                        found.append([m[0], cnt])
                    #check system same as Hospital Checker
                    out = ["None", 0]
                    if len(found) > 1:
                        for f in found:
                            if f[1] > out[1]:
                                out[0] = f[0]
                                out[1] = f[1]
                        if out[1] > 1:
                            Specialty_Id = out[0]
                    elif len(found) == 1:
                        for f in found:
                            out[0] = f[0]
                            out[1] = f[1]
                            Specialty_Id = out[0]
                                
        return Specialty_Id #return a specialty id matches about 80% of the time
    
#@method Insert_Emp
    def Insert_Emp(self, Recruiter, item):
        number = None
        email = None
        E_id = None
        if(item['contact_number'] is not None):
            number = self.route.spSelectEmpNumber(Recruiter[0], item['contact_number'])
            if(number is not None):
                E_id = number[0]
        if(number is None):
            if(item['contact_email'] is not None):
                email = self.route.spSelectEmpEmail(Recruiter[0], str(item['contact_email']).strip('.'))
                if(number is not None):
                    E_id = email[0]

        if(number is None and email is None):
            self.route.spInsertEmp(Recruiter[0], item['contact_name'], item['contact_number'], str(item['contact_email']).strip('.'))
            E_id = self.route.spSelectEmpNew()
        else:
            self.Update_Emp(item, E_id)
        
        return E_id

#@method Update Emp
    def Update_Emp(self, item, Emp_Id):
        self.route.spUpdateEmp(item['contact_name'], str(item['contact_email']).strip('.'), item['contact_number'], Emp_Id)
        
#@method Get_Loc
    def Get_Loc(self, item):
        loc_id = self.route.spSelectLocationId(item["job_state"], item['job_city'])
        if loc_id is not None:
            return loc_id[0]
        else:
            return loc_id

#@method Get_Abbrev
#@description: Translates State Names to an abbreviation when given a state name.
#               if given an already abbreviation state name then nothing is changed.
    def Get_Abbrev(self, statename):
        self.abbrev = states()
        val = self.abbrev.getAbbrev(statename)
        return val
        

"""
@name: piplines.py
@author: Rob Duff
@description: Takes items object created from each scraped page. Sends it to the database for storage.
"""
# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
#from services.routes import Routes
import nltk
nltk.download('all')
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
                matching = self.route.spSelectSpecialtyLike(words[0]) #returns any similar matches to the first word in the specialty
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
                            if f[1] > 1:
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
    def Get_Abbrev(self, state):
        us_state_abbrev = {
            'alabama': 'AL',
            'alaska': 'AK',
            'american Samoa': 'AS',
            'arizona': 'AZ',
            'arkansas': 'AR',
            'california': 'CA',
            'colorado': 'CO',
            'connecticut': 'CT',
            'delaware': 'DE',
            'district of Columbia': 'DC',
            'florida': 'FL',
            'georgia': 'GA',
            'guam': 'GU',
            'hawaii': 'HI',
            'idaho': 'ID',
            'illinois': 'IL',
            'indiana': 'IN',
            'iowa': 'IA',
            'kansas': 'KS',
            'kentucky': 'KY',
            'louisiana': 'LA',
            'maine': 'ME',
            'maryland': 'MD',
            'massachusetts': 'MA',
            'michigan': 'MI',
            'minnesota': 'MN',
            'mississippi': 'MS',
            'missouri': 'MO',
            'montana': 'MT',
            'nebraska': 'NE',
            'nevada': 'NV',
            'new Hampshire': 'NH',
            'new Jersey': 'NJ',
            'new Mexico': 'NM',
            'new York': 'NY',
            'north Carolina': 'NC',
            'north Dakota': 'ND',
            'northern Mariana Islands':'MP',
            'ohio': 'OH',
            'oklahoma': 'OK',
            'oregon': 'OR',
            'pennsylvania': 'PA',
            'puerto Rico': 'PR',
            'rhode Island': 'RI',
            'south Carolina': 'SC',
            'south Dakota': 'SD',
            'tennessee': 'TN',
            'texas': 'TX',
            'utah': 'UT',
            'vermont': 'VT',
            'virgin Islands': 'VI',
            'virginia': 'VA',
            'washington': 'WA',
            'west Virginia': 'WV',
            'wisconsin': 'WI',
            'wyoming': 'WY'
        }
        #check if state is already in abbreviated form
        if(len(state) > 2):
            abbrev = ''
            try:
                abbrev = us_state_abbrev[str(state).lower().strip()]
            except:
                abbrev = ''
        else:
            #flips the dictionary so short form is searchable
            abbrev_us_state = dict(map(reversed, us_state_abbrev.items()))
            #confirm abbreviation exists in the dict
            if state in abbrev_us_state:
                abbrev = state
            else:
                abbrev = ''

        return abbrev
        
#class for the primary sql server
class sqlserver_connection:
#connection parameters for the primary sql server
    def __init__(self):
        self.host = "70.179.173.208"
        self.port = 49172
        self.user = "rob"
        self.password = "supersecurepassword"
        self.db = "physemp"

#creates connection to the db
    def __connect__(self):
        self.conn = pymssql.connect(host=self.host, user=self.user, password=self.password, database=self.db, port=self.port, charset='utf8')
        self.cur = self.conn.cursor()

#disconnects connection from the db
    def __disconnect__(self):
        self.conn.close()

#runs sql commands requiring a value to be returned.
    def fetch(self, sql):
        self.__connect__()
        result = None
        try:
            self.cur.execute(sql)
            result = self.cur.fetchall()
        except Exception as e:
            print("Connection Fetch Failed: " + str(e) + str(sql))
        self.__disconnect__()
        #print("fetch returned: " + str(result) + " with sql: " + str(sql))
        return result
    
    def fetchone(self, sql):
        self.__connect__()
        result = None
        try:
            self.cur.execute(sql)
            result = self.cur.fetchone()
        except Exception as e:
            print("Connection Fetchone Failed: " + str(e) + str(sql))
        self.__disconnect__()
        #print("fetchone returned: " + str(result) + " with sql: " + str(sql))
        return result

#runs sql commands that do no require a return value
    def execute(self, sql, params):
        self.__connect__()
        try:
            self.cur.execute(sql, params)
            self.conn.commit()
        except Exception as e:
            print("Connection Execute Failed: " + str(e) + str(sql))
        #print("execute ran: " + str(sql) + str(params))
        self.__disconnect__()

#Class for all query routes to the server
class Routes:
    def __init__(self):
        self.conn = sqlserver_connection()
        
    def spInsertEmp(self, Recruiter_Id, Emp_name, Emp_email, Emp_number):
        sql = "EXEC spInsertEmp @Recruiter_Id = %s, @Emp_name = %s, @Emp_email = %s, @Emp_number = %s"
        self.conn.execute(sql, (Recruiter_Id, Emp_name, Emp_email, Emp_number))
    
    def spInsertJob(self, Recruiter_Id, Emp_id, Job_title, Hospital_type, Job_salary, Job_type, Job_state, Job_city, Job_address, Source_site, URL, Description, Hospital_id, Hospital_name, Ref_num, loc_id, Specialty, Specialty_Id):
        sql = """EXEC spInsertJob 
                    @Recruiter_Id=%s, 
                    @Emp_id=%s, 
                    @Job_title=%s, 
                    @Hospital_type=%s, 
                    @Job_salary=%s, 
                    @Job_type=%s,
                    @Job_state=%s,
                    @Job_city=%s, 
                    @Job_address=%s, 
                    @Source_site=%s, 
                    @URL=%s, 
                    @Description=%s, 
                    @Hospital_id=%s, 
                    @Hospital_name=%s,
                    @Ref_num=%s, 
                    @Loc_id=%s,
                    @Specialty=%s,
                    @Specialty_Id=%s"""
        self.conn.execute(sql,(Recruiter_Id, Emp_id, Job_title, Hospital_type, Job_salary, Job_type, Job_state, Job_city, Job_address, Source_site, URL, Description, Hospital_id, Hospital_name, Ref_num, loc_id, Specialty, Specialty_Id))

    def spInsertJobTimeItem(self, Job_Id, Date_scraped, Date_posted):
        sql = "EXEC spInsertJobTimeItem @Job_Id=%s, @Date_scraped=%s, @Date_posted=%s"
        self.conn.execute(sql, (Job_Id, Date_scraped, Date_posted))
    
    def spInsertRecruiter(self, Business_type, Business_name, Contact_name, Contact_number, Contact_email, Business_state, Business_city, Business_address, Business_zip, Business_website, hospital_id):
        sql = """EXEC spInsertRecruiter
                @Business_type = %s,
                @Business_name = %s,
                @Contact_name = %s,
                @Contact_number = %s,
                @Contact_email = %s,
                @Business_state = %s,
                @Business_city = %s,
                @Business_address = %s,
                @Business_zip = %s,
                @Business_website = %s,
                @hospital_id = %s"""
        self.conn.execute(sql, (Business_type, Business_name, Contact_name, Contact_number, Contact_email, Business_state, Business_city, Business_address, Business_zip, Business_website, hospital_id))
    
    def spSelectBusinessName(self, Business_name):
        sql = f"EXEC spSelectBusinessName @Business_name = '{Business_name}'"
        return self.conn.fetchone(sql)
    
    def spSelectEmpEmail(self, Recruiter_Id, Emp_email):
        sql = f"EXEC spSelectEmpEmail @Recruiter_Id={Recruiter_Id}, @Emp_email='{Emp_email}'"
        return self.conn.fetchone(sql)
    
    def spSelectEmpNew(self):
        sql = "EXEC spSelectEmpNew"
        return self.conn.fetchone(sql)
    
    def spSelectEmpNumber(self, Recruiter_Id, Emp_number):
        sql = f"EXEC spSelectEmpNumber @Recruiter_Id={Recruiter_Id}, @Emp_number='{Emp_number}'"
        return self.conn.fetchone(sql)
    
    def spSelectHospitalID(self, ID):
        sql = f"EXEC spSelectHospitalID @ID={ID}"
        return self.conn.fetch(sql)
    
    def spSelectHospitalLocation(self, City, State):
        sql = f"EXEC spSelectHospitalLocation @CITY='{City}', @STATE='{State}'"
        return self.conn.fetch(sql)
    
    def spSelectHospitalName(self, Name, City, State):
        sql = f"EXEC spSelectHospitalName @NAME='{Name}', @CITY='{City}', @STATE='{State}'"
        return self.conn.fetchone(sql)
    
    #returns a job_id
    def spSelectJobIdURL(self, URL):
        sql = f"EXEC spSelectJobIdURL @URL='{URL}'"
        return self.conn.fetchone(sql)
    
    #returns a url
    def spSelectJobURL(self, URL):
        sql = f"EXEC spSelectJobURL @URL='{URL}'"
        return self.conn.fetchone(sql)

    def spSelectLocationId(self, State_id, City):
        sql = f"EXEC spSelectLocationId @state_id ='{State_id}', @city_ascii='{City}'"
        return self.conn.fetchone(sql)
    
    def spSelectNewRecruiter(self):
        sql = "EXEC spSelectNewRecruiterId"
        return self.conn.fetchone(sql)
    
    def spSelectSpecialty(self, Specialty_name):
        sql = f"EXEC spSelectSpecialty @Specialty_name='{Specialty_name}'"
        return self.conn.fetchone(sql)
    
    def spSelectSpecialtyLike(self, Specialty_name):
        Specialty_name = "%" + Specialty_name + "%"
        sql = f"EXEC spSelectSpecialtyLike @Specialty_name='{Specialty_name}'"
        return self.conn.fetch(sql)
    
    def spUpdateEmp(self, Emp_name, Emp_email, Emp_number, Emp_id):
        sql = """EXEC spUpdateEmp 
                @Emp_name=%s,
                @Emp_email=%s,
                @Emp_number=%s,
                @Emp_id=%s"""
        self.conn.execute(sql, (Emp_name, Emp_email, Emp_number, Emp_id))
    
    def spUpdateJobAll(self, Job_title, Hospital_type, Job_salary, Job_type, Job_state, Job_city, Job_address, Source_site, URL, Description, Hospital_id, Hospital_name, Ref_num, loc_id, Specialty, Specialty_Id):
        sql = """EXEC spUpdateJobAll 
                    @Job_title=%s, 
                    @Hospital_type=%s, 
                    @Job_salary=%s, 
                    @Job_type=%s,
                    @Job_state=%s,
                    @Job_city=%s, 
                    @Job_address=%s, 
                    @Source_site=%s, 
                    @URL=%s, 
                    @Description=%s, 
                    @Hospital_id=%s, 
                    @Hospital_name=%s,
                    @Ref_num=%s, 
                    @Loc_id=%s,
                    @Specialty=%s,
                    @Specialty_Id=%s"""
        self.conn.execute(sql, (Job_title, Hospital_type, Job_salary, Job_type, Job_state, Job_city, Job_address, Source_site, URL, Description, Hospital_id, Hospital_name, Ref_num, loc_id, Specialty, Specialty_Id))
    
    def spUpdateJobNull(self, Job_title, Hospital_type, Job_salary, Job_type, Job_state, Job_city, Job_address, Source_site, URL, Description, Hospital_id, Hospital_name, Ref_num, loc_id, Specialty, Specialty_Id):
        sql = """EXEC spUpdateJobNull 
                    @Job_title=%s, 
                    @Hospital_type=%s, 
                    @Job_salary=%s, 
                    @Job_type=%s,
                    @Job_state=%s,
                    @Job_city=%s, 
                    @Job_address=%s, 
                    @Source_site=%s, 
                    @URL=%s, 
                    @Description=%s, 
                    @Hospital_id=%s, 
                    @Hospital_name=%s,
                    @Ref_num=%s, 
                    @Loc_id=%s,
                    @Specialty=%s,
                    @Specialty_Id=%s"""
        self.conn.execute(sql, (Job_title, Hospital_type, Job_salary, Job_type, Job_state, Job_city, Job_address, Source_site, URL, Description, Hospital_id, Hospital_name, Ref_num, loc_id, Specialty, Specialty_Id))
    
    def spUpdateRecruiter(self, Business_type, Business_name, Contact_name, Contact_number, Contact_email, Business_state, Business_city, Business_address, Business_zip, Business_website, hospital_id, Recruiter_Id):
        sql = """EXEC spUpdateRecruiter
                @Business_type = %s,
                @Business_name = %s,
                @Contact_name = %s,
                @Contact_number = %s,
                @Contact_email = %s,
                @Business_state = %s,
                @Business_city = %s,
                @Business_address = %s,
                @Business_zip = %s,
                @Business_website = %s,
                @hospital_id = %s,
                @Recruiter_Id = %s"""
        self.conn.execute(sql, (Business_type, Business_name, Contact_name, Contact_number, Contact_email, Business_state, Business_city, Business_address, Business_zip, Business_website, hospital_id, Recruiter_Id))
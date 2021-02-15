"""
@name: piplines.py
@author: Rob Duff
@description: Takes items object created from each scraped page. Sends it to the database for storage.
"""


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pymssql
import nltk
from nltk.corpus import stopwords

#https://stackoverflow.com/questions/43266482/scrapy-how-to-crawl-website-store-data-in-microsoft-sql-server-database/43266807


#Class PysEmpPipeline

class PhysempspidersPipeline:
    #Idk really what this does. Pretty sure when pipline is first called it creates a connection.
    #Unclear when this connection should be ended so it never is.
    def __init__(self):
        self.create_connection()

    #Simple method containing the connection info to the database.    
    def create_connection(self):
        self.conn = pymssql.connect(host='70.179.173.208', user='rob', password='verysecurepassword', database='physemp', port=49172,  charset='utf8')     
        self.cursor = self.conn.cursor()

    #Called whenever an item object is yeilded. Handles all logic for sending data to the DB
    def process_item(self, item, spider):
        #Gives empty values a null value so they can go into the db in a nice way. Storing as '' is no bueno.
        for i in item:
            if(item[i] == ''):
                item[i] = None
        
        if(self.Check_url(item['url'])==False):
            E_id = None
            #if the item we are processing does not have email or number info then we need to handle matching to the recruiter id based on its name. (ugly but necessary.)
            row = None
            try:
                self.cursor.execute("SELECT * FROM Recruiter WHERE Business_name=%s", item['business_name'])
                row = self.cursor.fetchone()
            except Exception as e:
                print('Error 1: Find Business_name Broke' + str(e))
            
            #The above try returns a single row from the DB of the matching recruiter id
            #If that row exists then we will insert a new job.
            if(row is not None):
                #format state name
                if(item['job_state'] is not None):
                    item['job_state'] = self.Get_Abbrev(str(item['job_state']))
                    #get location id if city AND state not null
                    if(item['job_city'] is not None):
                        item['Loc_id'] = self.Get_Loc(item)
                #update recruiter if email or number exists - add emp runs from this
                if(item['contact_email'] is not None or item['contact_number'] is not None):
                    #fix email
                    if item['contact_email'] is not None:
                        item['contact_email'] = str(item['contact_email']).strip('.')
                    E_id = self.Update_Recruiter(row, item)
                #add job with recruiter_id inserted into the job. row containts recruiter id
                #check if hospital info lines up with db entry. Update job info if necessary.
                hospital = self.Check_Hospital(item)
                if(hospital is not None):
                    if(item['job_state'] == None): item['job_state'] = hospital[3]
                    if(item['job_city'] == None): item['job_city'] = hospital[2]
                    if(item['job_address'] == None): item['job_address'] = hospital[1]
                    item['hospital_name'] = hospital[4]
                    item['hospital_id'] = hospital[0]
                    #inserts new job that contains a hospital_id
                    self.Insert_Job_H(row[0], item, E_id)
                else:
                    #inserts a new job that does not contain a hospital_id
                    self.Insert_Job(row[0], item, E_id) 
            #If there is no row then a new recruiter is added to the database. 
            else:
                #fix email
                item['contact_email'] = str(item['contact_email'].strip('.'))
                #format state name
                if(item['job_state'] is not None):
                    item['job_state'] = self.Get_Abbrev(str(item['job_state']))
                    #get location id if city AND state not null
                    if(item['job_city'] is not None):
                        item['Loc_id'] = self.Get_Loc(item)
                #make new recruiter
                id = self.Insert_Recruiter(item)
                #make new job with new recruiter
                #check if hospital info lines up with db entry. Update job info if necessary.
                hospital = self.Check_Hospital(item)
                if(hospital is not None):
                    if(item['job_state'] == None): item['job_state'] = hospital[3]
                    if(item['job_city'] == None): item['job_city'] = hospital[2]
                    if(item['job_address'] == None): item['job_address'] = hospital[1]
                    item['hospital_name'] = hospital[4]
                    item['hospital_id'] = hospital[0]
                    #inserts new job that contains a hospital_id
                    self.Insert_Job_H(id[0], item, E_id)
                else:
                    #inserts a new job that does not contain a hospital_id
                    self.Insert_Job(id[0], item, E_id)
        else:
            print('Updating Job')
            item['job_state'] = self.Get_Abbrev(str(item['job_state']))
            self.Update_Job_R(item)
        #Item is returned at the end dont ask me why 
        return item

#@method: Insert_Job_H
#description: inserts a new job into the database only if there is a matching hospital availiable.
    def Insert_Job_H(self, R_id, item, Emp_Id):
        #make new job with hospital
        try:
            sql = "INSERT INTO Jobs(Recruiter_Id, Emp_Id, Job_title, Specialty, Hospital_type, Job_salary, Job_type, Job_state, Job_city, Job_address, Source_site, URL, Description, Hospital_id, Hospital_name, Ref_num, Loc_id) VALUES (%d, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            self.cursor.execute(sql, (R_id, Emp_Id, item['title'], item['specialty'], item['hospital_type'], item['job_salary'], item['job_type'], item['job_state'], item['job_city'], item['job_address'], item['source_site'], item['url'], item['description'], item['hospital_id'], item['hospital_name'], item['Ref_num'], item['Loc_id']))
            self.conn.commit()
        except Exception as e:
            print('Error 8: Insert Job Hospital Broke' + item['url'] + str(e))
        #add time item to record last scrape
        self.Insert_Time(item)

#@method:Insert_Job
#@description: inserts a new job into the database. Must be given a recruiter id as an integer
    def Insert_Job(self, R_id, item, Emp_Id):
        #make new job
        try:
            sql = "INSERT INTO Jobs(Recruiter_Id, Emp_Id, Job_title, Specialty, Hospital_type, Job_salary, Job_type, Job_state, Job_city, Job_address, Source_site, URL, Description, Hospital_name, Ref_num, Loc_id) VALUES (%d, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            self.cursor.execute(sql, (R_id, Emp_Id, item['title'], item['specialty'], item['hospital_type'], item['job_salary'], item['job_type'], item['job_state'], item['job_city'], item['job_address'], item['source_site'], item['url'], item['description'], item['hospital_name'], item['Ref_num'], item['Loc_id']))
            self.conn.commit()
        except Exception as e:
            print('Error 3: Insert Job Broke' + item['url'] + ' Date:' + item['date_scraped'] + ' Error:' + str(e))
        #add time item to record last scrape
        self.Insert_Time(item)

#@method: Insert_Time
#@description: adds a time row to Job_Time_Item to track when an ad was scraped.
    def Insert_Time(self, item): 
        Job_id = None
        try:
            sqlID = "SELECT Job_Id FROM Jobs WHERE URL=%s"
            self.cursor.execute(sqlID, (item["url"]))
            Job_id = self.cursor.fetchone()
        except Exception as e:
            print("Error 12.1: find job ID broke" + str(e))
        if Job_id is not None:
            try:
                sql = "INSERT INTO Job_Time_Item(Job_Id, Date_scraped, Date_posted) VALUES (%s,%s,%s)"
                self.cursor.execute(sql, (Job_id, item["date_scraped"], item["date_posted"]))
                self.conn.commit()
            except Exception as e:
                print("Error 13: Insert Time Broke:" + str(e))
        else:
            print("I COULD NOT FIND JOB")

#@method: Update_Job
#@description: updates Job if url already exists.
    def Update_Job(self, item):
        try:
            sql = """UPDATE Jobs SET 
                    Job_title= COALESCE(Job_title,%s), 
                    Specialty= COALESCE(Specialty,%s), 
                    Hospital_type= COALESCE(Hospital_type,%s), 
                    Job_salary= COALESCE(Job_salary,%s), 
                    Job_type= COALESCE(Job_type,%s), 
                    Job_state= COALESCE(Job_state,%s), 
                    Job_city= COALESCE(job_city,%s), 
                    Job_address= COALESCE(Job_address,%s), 
                    Source_site= COALESCE(Source_site,%s),
                    Description= COALESCE(Description,%s),
                    Hospital_name= COALESCE(Hospital_name,%s),
                    Ref_num= COALESCE(Ref_num,%s),
                    Loc_id= COALESCE(Loc_id,%s)
                    WHERE URL=%s""" 
            self.cursor.execute(sql, (item['title'], item['specialty'], item['hospital_type'], item['job_salary'], item['job_type'], item['job_state'], item['job_city'], item['job_address'], item['source_site'], item['description'], item['hospital_name'], item['Ref_num'], item['Loc_id'], item['url']))
            self.conn.commit()
        except Exception as e:
            print("Error 11: Update Job Broke" + str(e))
        #add time item to record last scrape
        self.Insert_Time(item)

#@method: Update_Job_R
#@description: updates by replacing all data in the row matching a url
    def Update_Job_R(self, item):
        try:
            sql = """UPDATE Jobs SET 
                    Job_title= %s, 
                    Specialty= %s, 
                    Hospital_type= %s, 
                    Job_salary= %s, 
                    Job_type= %s, 
                    Job_state= %s, 
                    Job_city= %s, 
                    Job_address= %s, 
                    Source_site= %s,
                    Description= %s,
                    Hospital_name= %s,
                    Ref_num= %s,
                    Loc_id= %s
                    WHERE URL=%s""" 
            self.cursor.execute(sql, (item['title'], item['specialty'], item['hospital_type'], item['job_salary'], item['job_type'], item['job_state'], item['job_city'], item['job_address'], item['source_site'], item['description'], item['hospital_name'], item['Ref_num'],item['Loc_id'], item['url']))
            self.conn.commit()
        except Exception as e:
            print("Error 12: Update Job Replace Broke" + str(e))
        #add time item to record last scrape
        self.Insert_Time(item)

#@method: Insert_Recruiter
#@dexcription: inserts a new recruiter into the database.
    def Insert_Recruiter(self, item):
        #make new recruiter
        try:
            sql = "INSERT INTO Recruiter(Business_type, Business_name, Contact_name, Contact_number, Contact_email, Business_state, Business_city, Business_address, Business_zip, Business_website) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" 
            self.cursor.execute(sql, (item['business_type'], item['business_name'], item['contact_name'], item['contact_number'], item['contact_email'], item['business_state'], item['business_city'], item['business_address'], item['business_zip'], item['business_website']))
            self.conn.commit()
        except Exception as e:
                print('Error 4: Insert Recruiter Broke' + item['url'] + str(e))
        #return new Recruiter ID
        try:
            self.cursor.execute("SELECT Recruiter_Id FROM Recruiter WHERE Recruiter_Id = (SELECT MAX(Recruiter_Id) FROM Recruiter)")
            id = self.cursor.fetchone()
        except Exception as e:
                print('Error 5: Insert Recruiter Broke' + item['url'] + str(e))
        #return new id
        return id

#@method: Update_Recruiter
#@description: When given a recruiter id (R_id) it pulls the row and attempts to fill all empty columns. Then updates the row with the data.
    def Update_Recruiter(self, Recruiter, item):
        #if the recruiter does exist then fill empty fields with new info. Save old info
        E_id = None
        if(Recruiter is not None):   
            try:
                update = """UPDATE Recruiter SET 
                            Business_type= COALESCE(Business_type,%s), 
                            business_name= COALESCE(Business_name,%s), 
                            contact_name= COALESCE(Contact_name,%s), 
                            contact_number= COALESCE(Contact_number,%s), 
                            contact_email= COALESCE(Contact_email,%s), 
                            business_state= COALESCE(business_state,%s), 
                            business_city= COALESCE(business_city,%s), 
                            business_address= COALESCE(business_address,%s), 
                            business_zip= COALESCE(business_zip,%s), 
                            business_website= COALESCE(business_website,%s) 
                            WHERE Recruiter_Id=%s""" 
                
                self.cursor.execute(update, (item['business_type'], item['business_name'], item['contact_name'], item['contact_number'], item['contact_email'], item['business_state'], item['business_city'], item['business_address'], item['business_zip'], item['business_website'], Recruiter[0]))
                self.conn.commit()
                print("UPDATE Recruiter RAN --------------------------------------------")
            except Exception as e:
                print('Error 9: Update Recruiter Failed to find Recruiter' + str(e))
            
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
        test = None
        try:
            self.cursor.execute("SELECT URL FROM JOBS WHERE URL=%s", url)
            test = self.cursor.fetchone()
        except Exception as e:
                print('Error 6: Check URL Broke' + url + str(e))
        
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
            try:
                self.cursor.execute("SELECT ID, ADDRESS, CITY, STATE, NAME FROM Hospitals WHERE NAME=%s AND CITY=%s", (item['hospital_name'], item['job_city']))
                hospital = self.cursor.fetchone()
            except Exception as e:
                print('Error 7: Check_hospital broke' + item['url'] + str(e))
        
        if(item['job_address'] is not None and hospital is None):
            try:
                self.cursor.execute("SELECT ID, ADDRESS, CITY, STATE, NAME FROM Hospitals WHERE ADDRESS=%s", (item['job_address']))
                hospital = self.cursor.fetchone()
            except Exception as e:
                print('Error 7: Check_hospital broke' + item['url'] + str(e))
        
        if(hospital is None):
            #set up stopwords
            more_stopwords = set(())
            stoplist = set(stopwords.words('english')) | more_stopwords
            hospitals = None
            #select hospitals in the same location as the job
            if item['Loc_id'] is not None:
                try:
                    sqlHos = "SELECT NAME, CITY, STATE, ID FROM Hospitals WHERE City = %s AND State = %s"
                    self.cursor.execute(sqlHos, (item["job_city"], item["job_state"]))
                    hospitals = self.cursor.fetchall()
                except Exception as e:
                    print('Error 7: Check_hospital broke' + item['url'] + str(e))

            #of those hospitals lets try to narrow down the right one from the description
            found = [] #to hold the best matching
            desc = str(item['description']).lower().replace("’","").replace("'","").replace(">"," ").replace("<", " ")

            if hospitals is not None:
                for h in hospitals:
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
                        found.append([name, cnt, h[3]])
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
                    try:
                        self.cursor.execute("SELECT NAME, CITY, STATE, ID FROM Hospitals WHERE ID=%s", (out[2]))
                        hospital = self.cursor.fetchone()
                    except Exception as e:
                        print('Error 7: Check_hospital broke' + item['url'] + str(e))
        #return hospital variable at the end, None if nothing was found or contains a list of Name, City, State, ID
        return hospital

#@method Insert_Emp
    def Insert_Emp(self, Recruiter, item):
        number = None
        email = None
        E_id = None
        if(item['contact_number'] is not None):
            try:
                self.cursor.execute("SELECT * FROM Employee WHERE Recruiter_Id=%s AND Emp_number=%s", (Recruiter[0], item['contact_number']))
                number = self.cursor.fetchone()
                if(number is not None):
                    E_id = number[0]
            except Exception as e:
                print('Error 10.1: Insert_Emp number broke ' + str(item['contact_number']) + str(e))
        if(number is None):
            if(item['contact_email'] is not None):
                try:
                    self.cursor.execute("SELECT * FROM Employee WHERE Recruiter_Id=%s AND Emp_email=%s", (Recruiter[0], str(item['contact_email']).strip('.')))
                    email = self.cursor.fetchone()
                    if(number is not None):
                        E_id = email[0]
                except Exception as e:
                    print('Error 10.2: Insert_Emp email broke ' + str(item['contact_email']) + str(e))

        if(number is None and email is None):
            try:
                self.cursor.execute("INSERT INTO Employee(Recruiter_Id, Emp_name, Emp_number, Emp_email) VALUES(%s,%s,%s,%s)", (Recruiter[0], item['contact_name'], item['contact_number'], str(item['contact_email']).strip('.')))
                self.conn.commit()
            except Exception as e:
                print('Error 10.3: Insert_Emp broke ' + str(e))
            try:
                self.cursor.execute("SELECT Emp_Id FROM Employee WHERE Emp_Id = (SELECT MAX(Emp_Id) FROM Employee)")
                E_id = self.cursor.fetchone()
            except Exception as e:
                    print('Error 10.4: Get Employee_ID Broke' + item['url'] + str(e))
        else:
            self.Update_Emp(item, E_id)
        
        return E_id

#@method Update Emp
    def Update_Emp(self, item, Emp_Id):
        try:
            self.cursor.execute("UPDATE Employee SET Emp_name = COALESCE(Emp_name,%s), Emp_email = COALESCE(Emp_email,%s), Emp_number = COALESCE(Emp_number,%s) WHERE Emp_Id=%s", (item['contact_name'], str(item['contact_email']).strip('.'), item['contact_number'], Emp_Id))
            self.conn.commit()
        except Exception as e:
            print('Error 11: Update_Emp broke' + str(e))
        
#@method Get_Loc
    def Get_Loc(self, item):
        loc_id = None
        try:
            self.cursor.execute("SELECT Locations.loc_id FROM Locations WHERE state_id = %s AND city_ascii=%s", (item["job_state"], item['job_city']))
            loc_id = self.cursor.fetchone()
        except Exception as e:
            print("Error 14: Get_loc broke" + str(e))
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
        

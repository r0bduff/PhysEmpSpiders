"""
@name: piplines.py
@author: Rob Duff
@description: Takes items object created from each scraped page. Sends it to the database for storage.
"""


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pymssql


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

        #if the item we are processing does not have email or number info then we need to handle matching to the recruiter id based on its name. (ugly but necessary.)
        if(item['contact_email']==None and item['contact_number']==None):
            try:
                self.cursor.execute("SELECT * FROM Recruiter WHERE Business_name=%s", item['business_name'])
                row = self.cursor.fetchone()
                self.Update_Recruiter(row, item)
            except Exception as e:
                print('Error 1: Find Business_name Broke' + str(e))
        #if there is a contact email or number then we can identify the recruiter via that. This is best.
        else:
            try:
                self.cursor.execute("SELECT * FROM Recruiter WHERE Contact_email=%s OR Contact_number=%s AND Contact_email IS NOT NULL AND Contact_number IS NOT NULL", (item['contact_email'], item['contact_number']))
                row = self.cursor.fetchone()
                self.Update_Recruiter(row, item)
            except Exception as e:
                print('Error 2: Find email/number Broke' + str(e))

        #The above if-else returns a single row from the DB of the matching recruiter id
        #If that row exists then we will insert a new job.
        if(row is not None):
            #add job with recruiter_id inserted into the job. row containts recruiter id
            if(self.Check_url(item['url'])==False):
                #check if hospital info lines up with db entry. Update job info if necessary.
                hospital = self.Check_Hospital(item)
                if(hospital is not None):
                    if(item['job_state'] == None): item['job_state'] = hospital[3]
                    if(item['job_city'] == None): item['job_city'] = hospital[2]
                    if(item['job_address'] == None): item['job_address'] = hospital[1]
                    item['hospital_id'] = hospital[0]
                    #inserts new job that contains a hospital_id
                    self.Insert_Job_H(row[0], item)
                else:
                    #inserts a new job that does not contain a hospital_id
                    self.Insert_Job(row[0], item)
            else:
                print('Job already exists in DB')
        #If there is no row then a new recruiter is added to the database. 
        else:
            #make new recruiter
            id = self.Insert_Recruiter(item)
            #make new job with new recruiter
            if(self.Check_url(item['url'])==False):
                #check if hospital info lines up with db entry. Update job info if necessary.
                hospital = self.Check_Hospital(item)
                if(hospital is not None):
                    if(item['job_state'] == None): item['job_state'] = hospital[3]
                    if(item['job_city'] == None): item['job_city'] = hospital[2]
                    if(item['job_address'] == None): item['job_address'] = hospital[1]
                    item['hospital_id'] = hospital[0]
                    #inserts new job that contains a hospital_id
                    self.Insert_Job_H(id[0], item)
                else:
                    #inserts a new job that does not contain a hospital_id
                    self.Insert_Job(id[0], item)
            #the job already exists in the db
            else:
                print('Job already exists in DB')
        #self.conn.close()
        #Item is returned at the end dont ask me why 
        return item

#@method: Insert_Job_H
#description: inserts a new job into the database only if there is a matching hospital availiable.
    def Insert_Job_H(self, R_id, item):
        #make new job with hospital
        try:
            sql = "INSERT INTO Jobs(Recruiter_Id, Job_title, Specialty, Hospital_type, Job_salary, Job_type, Job_state, Job_city, Job_address, Date_posted, Date_scraped, Source_site, URL, Description, Hospital_id, Hospital_name) VALUES (%d, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            self.cursor.execute(sql, (R_id, item['title'], item['specialty'], item['hospital_type'], item['job_salary'], item['job_type'], item['job_state'], item['job_city'], item['job_address'], item['date_posted'], item['date_scraped'], item['source_site'], item['url'], item['description'], item['hospital_id'], item['hospital_name']))
            self.conn.commit()
        except Exception as e:
                print('Error 8: Insert Job Hospital Broke' + item['url'] + str(e))

#@method:Insert_Job
#@description: inserts a new job into the database. Must be given a recruiter id as an integer
    def Insert_Job(self, R_id, item):
        #make new job
        try:
            sql = "INSERT INTO Jobs(Recruiter_Id, Job_title, Specialty, Hospital_type, Job_salary, Job_type, Job_state, Job_city, Job_address, Date_posted, Date_scraped, Source_site, URL, Description, Hospital_name) VALUES (%d, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            self.cursor.execute(sql, (R_id, item['title'], item['specialty'], item['hospital_type'], item['job_salary'], item['job_type'], item['job_state'], item['job_city'], item['job_address'], item['date_posted'], item['date_scraped'], item['source_site'], item['url'], item['description'], item['hospital_name']))
            self.conn.commit()
        except Exception as e:
                print('Error 3: Insert Job Broke' + item['url'] + ' Date:' + item['date_scraped'] + ' Error:' + str(e))

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
        if(Recruiter is not None):
            if((Recruiter[4] is not None and Recruiter[4] != item['contact_number'] and item['contact_number'] is not None) or (Recruiter[5] is not None and Recruiter[5] != item['contact_email'] and item['contact_email'] is not None)):
                self.Insert_Emp(Recruiter, item)
               
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
                            WHERE Recruiter_Id=""" + str(Recruiter[0]) 
                
                self.cursor.execute(update, (Recruiter[1], Recruiter[2], Recruiter[3], Recruiter[4], Recruiter[5],Recruiter[6], Recruiter[7], Recruiter[8], Recruiter[9], Recruiter[10]))
                self.conn.commit()
            except Exception as e:
                print('Error 9: Update Recruiter Failed to find Recruiter' + str(e))

#@method: Check_url
#@description: checks to verify that the url of a job listing does not already exist in the db.
#              This eliminates duplicates at the cost of potential preformance in the long run?    
    def Check_url(self, url):
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
                self.cursor.execute("SELECT ID, ADDRESS, CITY, STATE FROM Hospitals WHERE NAME=%s AND CITY=%s", (item['hospital_name'], item['job_city']))
                hospital = self.cursor.fetchone()
            except Exception as e:
                print('Error 7: Check_hospital broke' + item['url'] + str(e))

        if(item['business_website'] is not None and hospital is None):
            try:
                self.cursor.execute("SELECT ID, ADDRESS, CITY, STATE FROM Hospitals WHERE WEBSITE=%s", (item['business_website']))
                hospital = self.cursor.fetchone()
            except Exception as e:
                print('Error 7: Check_hospital broke' + item['url'] + str(e))
    
        if(item['contact_number'] is not None and hospital is None):
            try:
                self.cursor.execute("SELECT ID, ADDRESS, CITY, STATE FROM Hospitals WHERE TELEPHONE=%s", (item['contact_number']))
                hospital = self.cursor.fetchone()
            except Exception as e:
                print('Error 7: Check_hospital broke' + item['url'] + str(e))
        
        if(item['job_address'] is not None and hospital is None):
            try:
                self.cursor.execute("SELECT ID, ADDRESS, CITY, STATE FROM Hospitals WHERE ADDRESS=%s", (item['job_address']))
                hospital = self.cursor.fetchone()
            except Exception as e:
                print('Error 7: Check_hospital broke' + item['url'] + str(e))
         
        return hospital

#@method Insert_Emp
    def Insert_Emp(self, item, Recruiter):
        number = None
        email = None
        try:
            self.cursor.execute("SELECT * FROM Employee WHERE Recruiter_Id=%s AND Emp_number=%s", (Recruiter[0], item['contact_number']))
            number = self.cursor.fetchone()
            E_id = number[0]
        except Exception as e:
            print('Error 10: Insert_Emp broke' + str(e))
        
        try:
            self.cursor.execute("SELECT * FROM Employee WHERE Recruiter_Id=%s AND Emp_email=%s", (Recruiter[0], item['contact_email']))
            email = self.cursor.fetchone()
            E_id = email[0]
        except Exception as e:
            print('Error 10: Insert_Emp broke' + str(e))

        if(number is None and email is None):
            try:
                self.cursor.execute("INSERT INTO Employee Recruiter_Id, Emp_name, Emp_number, Emp_email VALUES(%s,%s,%s,%s)", (Recruiter[0], item['contact_name'], item['contact_number'], item['contact_email']))
                self.conn.commit()
            except Exception as e:
                print('Error 10: Insert_Emp broke' + str(e))
        else:
            self.Update_Emp(item, E_id)

#@method Update Emp
    def Update_Emp(self, item, Emp_Id):
        try:
            self.cursor.execute("UPDATE Employee SET Emp_name = COALESCE(Emp_name,%s), Emp_email = COALESCE(Emp_email,%s), Emp_number = COALESCE(Emp_number,%s) WHERE Emp_Id=%s", (item['contact_name'], item['contact_email'], item['contact_number'], Emp_Id))
            self.conn.commit()
        except Exception as e:
            print('Error 11: Update_Emp broke' + str(e))
        
        

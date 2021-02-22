#NOT FUNCTIONAL

from connections import sqlserver_connection as connection

class Routes:
    def __init__(self):
        self.conn = connection()
        
    def spInsertEmp(self, Recruiter_Id, Emp_name, Emp_email, Emp_number):
        sql = f"EXEC spInsertEmp @Recruiter_Id = {Recruiter_Id}, @Emp_name = {Emp_name}, @Emp_email = {Emp_email}, @Emp_number = {Emp_number}"
        self.conn.execute(sql)
    
    def spInsertJob(self, Recruiter_Id, Emp_id, Job_title, Hospital_type, Job_salary, Job_type, Job_state, Job_city, Job_address, Source_site, URL, Description, Hospital_id, Hospital_name, Ref_num, loc_id, Specialty_Id):
        sql = f"""EXEC spInsertJob 
                    @Recruiter_Id={Recruiter_Id}, 
                    @Emp_Id={Emp_Id}, 
                    @Job_title={Job_title}, 
                    @Hospital_type={Hospital_type}, 
                    @Job_salary={Job_salary}, 
                    @Job_type={Job_type},
                    @Job_state={Job_state},
                    @Job_city={Job_city}, 
                    @Job_address={Job_address}, 
                    @Source_site={Source_site}, 
                    @URL={URL}, 
                    @Description={Description}, 
                    @Hospital_id={Hospital_id}, 
                    @Hospital_name={Hospital_name},
                    @Ref_num={Ref_num}, 
                    @Loc_id={loc_id},
                    @Specialty_Id={Specialty_Id}"""
        self.conn.execute(sql)

    def spInsertJobTimeItem(self, Job_Id, Date_scraped, Date_posted):
        sql = f"EXEC spInsertJobTimeItem @Job_Id={Job_Id}, @Date_scraped={Date_scraped}, @Date_posted={Date_posted}"
        self.conn.execute(sql)
    
    def spInsertRecruiter(self, Business_type, Business_name, Contact_name, Contact_email, Business_state, Business_city, Business_address, Business_zip, Business_website, hospital_id):
        sql = f"""EXEC spInsertRecruiter
                @Business_type = {Business_type},
                @Business_name = {Business_name},
                @Contact_name = {Contact_name},
                @Contact_email = {Contact_email},
                @Business_state = {Business_state},
                @Business_city = {Business_city},
                @Business_address = {Business_address},
                @Business_zip = {Business_zip},
                @Business_website = {Business_website},
                @hospital_id = {Hospital_id}"""
        self.conn.execute(sql)
    
    def spSelectBusinessName(self, Business_name):
        sql = f"EXEC spSelectBusinessName @Business_name = {Business_name}"
        return self.conn.fetch(sql)
    
    def spSelectEmpEmail(self, Recruiter_Id, Emp_email):
        sql = f"EXEC spSelectEmpEmail @Recruiter_Id={Recruiter_Id}, @Emp_email={Emp_email}"
        return self.conn.fetch(sql)
    
    def spSelectEmpNew(self):
        sql = "EXEC spSelectEmpNew"
        return self.conn.fetch(sql)
    
    def spSelectEmpNumber(self, Recruiter_Id, Emp_number):
        sql = f"EXEC spSelectEmpNumber @Recruiter_Id={Recruiter_Id}, @Emp_number={Emp_number}"
        return self.conn.fetch(sql)
    
    def spSelectHospitalID(self, ID):
        sql = f"EXEC spSelectHospitalID @ID={ID}"
        return self.conn.fetch(sql)
    
    def spSelectHospitalLocation(self, City, State):
        sql = f"EXEC spSelectHospitalLocation @CITY={City}, @STATE={State}"
        return self.conn.fetch(sql)
    
    def spSelectHospitalName(self, Name, City, State):
        sql = f"EXEC spSelectHospitalName @NAME={Name}, @CITY={City}, @STATE={State}"
        return self.conn.fetch(sql)
    
    #returns a job_id
    def spSelectJobIdURL(self, URL):
        sql = f"EXEC spSelectJobIdURL @URL={URL}"
        return self.conn.fetch(sql)
    
    #returns a url
    def spSelectJobURL(self, URL):
        sql = f"EXEC spSelectJobURL @URL={URL}"
        return self.conn.fetch(sql)

    def spSelectLocationId(self, City, State_id):
        sql = f"EXEC spSelectLocationId @state_id ={State_id}, @city_ascii={City}"
        return self.conn.fetch(sql)
    
    def spSelectNewRecruiter(self):
        sql = "EXEC spSelectNewRecruiter"
        return self.conn.fetch(sql)
    
    def spUpdateEmp(self, Emp_name, Emp_email, Emp_number, Emp_id):
        sql = f"""EXEC spUpdateEmp 
                @Emp_name={Emp_name},
                @Emp_email={Emp_email},
                @Emp_number={Emp_number},
                @Emp_id={Emp_id}"""
        self.conn.execute(sql)
    
    def spUpdateJobAll(self, Emp_id, Job_title, Hospital_type, Job_salary, Job_type, Job_state, Job_city, Job_address, Source_site, URL, Description, Hospital_id, Hospital_name, Ref_num, loc_id, Specialty_Id):
        sql = f"""EXEC spUpdateJobAll
                    @Emp_Id={Emp_Id}, 
                    @Job_title={Job_title}, 
                    @Hospital_type={Hospital_type}, 
                    @Job_salary={Job_salary}, 
                    @Job_type={Job_type},
                    @Job_state={Job_state},
                    @Job_city={Job_city}, 
                    @Job_address={Job_address}, 
                    @Source_site={Source_site}, 
                    @URL={URL}, 
                    @Description={Description}, 
                    @Hospital_id={Hospital_id}, 
                    @Hospital_name={Hospital_name},
                    @Ref_num={Ref_num}, 
                    @Loc_id={loc_id},
                    @Specialty_Id={Specialty_Id}"""
        self.conn.execute(sql)
    
    def spUpdateJobNull(self, Emp_id, Job_title, Hospital_type, Job_salary, Job_type, Job_state, Job_city, Job_address, Source_site, URL, Description, Hospital_id, Hospital_name, Ref_num, loc_id, Specialty_Id):
        sql = f"""EXEC spUpdateJobNull
                    @Emp_Id={Emp_Id}, 
                    @Job_title={Job_title}, 
                    @Hospital_type={Hospital_type}, 
                    @Job_salary={Job_salary}, 
                    @Job_type={Job_type},
                    @Job_state={Job_state},
                    @Job_city={Job_city}, 
                    @Job_address={Job_address}, 
                    @Source_site={Source_site}, 
                    @URL={URL}, 
                    @Description={Description}, 
                    @Hospital_id={Hospital_id}, 
                    @Hospital_name={Hospital_name},
                    @Ref_num={Ref_num}, 
                    @Loc_id={loc_id},
                    @Specialty_Id={Specialty_Id}"""
        self.conn.execute(sql)
    
    def spUpdateRecruiter(self, Business_type, Business_name, Contact_name, Contact_email, Business_state, Business_city, Business_address, Business_zip, Business_website, hospital_id, Recruiter_Id):
        sql = f"""EXEC spUpdateRecruiter
                @Business_type = {Business_type},
                @Business_name = {Business_name},
                @Contact_name = {Contact_name},
                @Contact_email = {Contact_email},
                @Business_state = {Business_state},
                @Business_city = {Business_city},
                @Business_address = {Business_address},
                @Business_zip = {Business_zip},
                @Business_website = {Business_website},
                @hospital_id = {Hospital_id},
                @Recruiter_id = {Recruiter_id}"""
        self.conn.execute(sql)
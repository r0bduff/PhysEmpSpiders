#Stores all queries to the sql server

from .connections import sqlserver_connection as connection

#Class for all query routes to the server
class Routes:
    def __init__(self):
        self.conn = connection()
        
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
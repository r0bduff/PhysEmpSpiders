import pymssql
import csv

conn = pymssql.connect(host='70.179.173.208', user='rob', password='verysecurepassword', database='physemp', port=49172,  charset='utf8')
cursor = conn.cursor()

selectall = 'SELECT Recruiter_Id, COntact_name, Contact_number, Contact_email FROM Recruiter'
cursor.execute(selectall)
Recruiters = cursor.fetchall()

for emp in Recruiters:
    cont = True
    if(emp[2] is not None):
        cursor.execute("SELECT * FROM Employee WHERE Emp_number=%s", (emp[2]))
        test = cursor.fetchone()
        if(test is not None):
            cont = False
            print('number exists')
            if(test[3] is None and emp[3] is not None):
                cursor.execute("UPDATE Employee SET Emp_email=%s", (emp[3]))
                conn.commit()
                print('updated email')
    
    if(emp[3] is not None):
        cursor.execute("SELECT * FROM Employee WHERE Emp_email=%s", (emp[3]))
        if(cursor.fetchone() is not None):
            cont = False
            print('email exists')
            if(test[2] is None and emp[2] is not None):
                cursor.execute("UPDATE Employee SET Emp_number=%s", (emp[2]))
                conn.commit()
                print('updated number')
    
    if(cont == True):
        if(emp[2] is None and emp[3] is None):
            print(str(emp[0]) + 'no info to enter')
        else:
            insert = "INSERT INTO Employee(Recruiter_Id, Emp_name, Emp_email, Emp_number) VALUES(%s,%s,%s,%s)"
            print('Inserting ID: %s, Name: %s, Number: %s, Email: %s', (emp[0], emp[1], emp[2], emp[3]))
            cursor.execute(insert, (emp[0], emp[1], emp[2], emp[3]))
            conn.commit()

cursor.close()
conn.close()   
        
print('end')

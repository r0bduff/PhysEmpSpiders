#Holds all connection info the the sql server

import pymssql
import psycopg2

class postgres_connection:
    def __init__(self):
        self.host = "ec2-52-7-178-38.compute-1.amazonaws.com"
        self.port = 5432
        self.user = "robert"
        self.password = "pf134c00b987afe51b1f99458a51db59d68405087ffca810e5c734c490212f49b"
        self.db = "scraper"

#creates connection to the db
    def __connect__(self):
        self.conn = psycopg2.connect(host=self.host, user=self.user, password=self.password, database=self.db, port=self.port)
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



#class for the primary sql server
class sqlserver_connection:
#connection parameters for the primary sql server
    def __init__(self):
        self.host = "76.92.246.48"
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
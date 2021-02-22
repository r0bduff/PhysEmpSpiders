#connection class 
import pymssql

#class for the primary sql server
class sqlserver_connection:
#connection parameters for the primary sql server
    def __init__(self):
        self.host = "70.179.173.208"
        self.port = 49172
        self.user = "rob"
        self.password = "verysecurepassword"
        self.db = "physemp"

#creates connection to the db
    def __connect__(self):
        self.conn = pymysql.connect(host=self.host, user=self.user, password=self.password, db=self.db, port=self.port, cursorclass=pymysql.cursors.DictCursor)
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
            print("SQL:" + str(sql) + "Connection Fetch Failed: " + str(e))
        self.__disconnect__()
        return result

#runs sql commands that do no require a return value
    def execute(self, sql):
        self.__connect__()
        try:
            self.cur.execute(sql)
        except Exception as e:
            print("Connection Execute Failed: " + str(e))
        self.__disconnect__()
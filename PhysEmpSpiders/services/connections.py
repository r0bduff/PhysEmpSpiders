#connection class 
import pymssql

class sqlserver_connection:
    def __init__(self):
        self.host = "70.179.173.208"
        self.port = 49172
        self.user = "rob"
        self.password = "verysecurepassword"
        self.db = "physemp"

    def __connect__(self):
        self.conn = pymysql.connect(host=self.host, user=self.user, password=self.password, db=self.db, port=self.port, cursorclass=pymysql.cursors.DictCursor)
        self.cur = self.conn.cursor()

    def __disconnect__(self):
        self.conn.close()

    def fetch(self, sql):
        self.__connect__()
        self.cur.execute(sql)
        result = self.cur.fetchall()
        self.__disconnect__()
        return result

    def execute(self, sql):
        self.__connect__()
        self.cur.execute(sql)
        self.__disconnect__()
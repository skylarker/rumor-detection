from mysql.utils.error_handling import ErrorHandling
from mysql.db.mysql_dal import MySQLConnector
#from warnings import resetwarnings
from warnings import filterwarnings
import MySQLdb as MySQL
import time
import sys


class Execute(object):
    def __init__(self):
        self.mysql = MySQLConnector()
        self.error = ErrorHandling()
        filterwarnings('ignore', category=MySQL.Warning)

    def execute_query(self, cursor, query):

        attempts = 0
        while True:
            try:
                cursor.execute(query)
                break
            except MySQL.Error, e:
                attempts += 1
                self.error.warn(" *MYSQL Corpus DB ERROR on %s:\n%s (%d attempt)" % (query, e, attempts))
                time.sleep(MySQLConnector.MYSQL_TIMEOUT)
                if attempts > MySQLConnector.MAX_ATTEMPTS:
                    sys.exit(1)
        #resetwarnings()
        return True
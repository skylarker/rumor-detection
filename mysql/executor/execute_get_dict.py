from mysql.utils.error_handling import ErrorHandling
from mysql.db.mysql_dal import MySQLConnector
import MySQLdb
import time
import sys


class ExecuteGetDict(object):
    def __init__(self):
        self.mysql = MySQLConnector()
        self.error = ErrorHandling()

    def execute_get_dict(self, dict_cursor, query, warn_query=False):
        """
        Executes a given query, returns results as a list of dicts.

        :param dict_cursor:
        :param query:
        :param warn_query:
        """
        #if warn_query:
            #self.error.warn("SQL (DictCursor) QUERY: %s" % query)
        data = []
        attempts = 0
        while True:
            try:
                dict_cursor.execute(query)
                data = dict_cursor.fetchall()
                break
            except MySQLdb.Error, e:
                attempts += 1
                self.error.warn(" *MYSQL Corpus DB ERROR on %s:\n%s (%d attempt)" % (query, e, attempts))
                time.sleep(MySQLConnector.MYSQL_TIMEOUT)
                (db_conn, db_cursor, dict_cursor) = self.mysql.connect()  # TODO connection parameters
                if attempts > MySQLConnector.MAX_ATTEMPTS:
                    sys.exit(1)
        return data
from mysql.db.mysql_dal import MySQLConnector
from mysql.utils.error_handling import ErrorHandling
import MySQLdb
import time
import sys


class ExecuteGetList(object):
    def __init__(self):
        self.mysql = MySQLConnector()
        self.error = ErrorHandling()

    def execute_get_list(self, db, db_cursor, sql, warn_query=False):
        """
        Executes a given query, returns results as a list of lists.

        :param db:
        :param db_cursor:
        :param sql:
        :param warn_query:
        """
        if warn_query:
            self.error.warn("SQL QUERY: %s" % sql)
        data = []
        attempts = 0
        while True:
            try:
                db_cursor.execute(sql)
                data = db_cursor.fetchall()
                break
            except MySQLdb.Error, e:
                attempts += 1
                self.error.warn(" *MYSQL Corpus DB ERROR on %s:\n%s (%d attempt)" % (sql, e, attempts))
                time.sleep(MySQLConnector.MYSQL_TIMEOUT)
                (db_conn, db_cursor, dict_cursor) = self.mysql.db_connect(db)
                if attempts > MySQLConnector.MAX_ATTEMPTS:
                    sys.exit(1)
        return data
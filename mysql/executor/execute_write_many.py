from mysql.db.mysql_dal import MySQLConnector
from mysql.utils.error_handling import ErrorHandling
import MySQLdb
import time
import sys


class ExecuteWriteMany(object):
    def __init__(self):
        self.mysql = MySQLConnector()
        self.error = ErrorHandling()

    def execute_write_many(self, db, db_conn, query, rows, write_cursor=None, warn_query=False):
        """
        Executes a write query

        :param db:
        :param db_conn:
        :param query:
        :param rows:
        :param write_cursor:
        :param warn_query:
        """
        if warn_query:
            self.error.warn("SQL (write many) QUERY: %s" % query)
        if not write_cursor:
            write_cursor = db_conn.cursor()
        attempts = 0
        while 1:
            try:
                write_cursor.executemany(query, rows)
                break
            except MySQLdb.Error, e:
                attempts += 1
                self.error.warn(" *MYSQL Corpus DB ERROR on %s:\n%s (%d attempt)" % (query, e, attempts))
                time.sleep(MySQLConnector.MYSQL_TIMEOUT)
                (db_conn, db_cursor, dict_cursor) = self.mysql.db_connect(db)
                write_cursor = db_conn.cursor()
                if attempts > MySQLConnector.MAX_ATTEMPTS:
                    sys.exit(1)
        return write_cursor
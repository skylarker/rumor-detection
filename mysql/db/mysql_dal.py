from mysql.utils.error_handling import ErrorHandling
import MySQLdb
import time
import sys


class MySQLConnector(object):
    """
     This class is used to establish connection to MySQL server on local machine/AWS ec2 instance.

    """
    MAX_ATTEMPTS = 5  # max number of times to try a query before exiting
    MYSQL_TIMEOUT = 4  # number of seconds to wait before trying a query again in case of a failure

    def __init__(self, user=None, passwd=None, host=None, port=None):
        self.user = user
        self.passwd = passwd
        self.host = host
        self.port = port
        self.error = ErrorHandling()

    def connect(self):
        """
        Makes a connection with MySQL server. Returns tuple of (conn, db_cursor, dict_cursor)

        """
        conn = None
        attempts = 0
        while True:
            try:
                conn = MySQLdb.connect(host=self.host,
                                       port=self.port,
                                       user=self.user,
                                       passwd=self.passwd)
                conn.autocommit(True)
                break
            except MySQLdb.Error, e:
                attempts += 1
                self.error.warn(" *MYSQL Connection ERROR:%s\n (%d attempt)" % (e, attempts))
                time.sleep(MySQLConnector.MYSQL_TIMEOUT)
                if attempts > MySQLConnector.MAX_ATTEMPTS:
                    sys.exit(1)
        return conn

    def db_connect(self, db=None):
        """
        Connects to specified database. Returns tuple of (db_conn, db_cursor, dict_cursor)
        :param db: database

        """
        db_conn = None
        attempts = 0
        while True:
            try:
                db_conn = MySQLdb.connect(host=self.host,
                                          port=self.port,
                                          user=self.user,
                                          passwd=self.passwd,
                                          db=db)
                db_conn.autocommit(True)
                break
            except MySQLdb.Error, e:
                attempts += 1
                self.error.warn(" *MYSQL Connect ERROR on db:%s\n%s\n (%d attempt)" % (db, e, attempts))
                time.sleep(MySQLConnector.MYSQL_TIMEOUT)
                if attempts > MySQLConnector.MAX_ATTEMPTS:
                    sys.exit(1)
        return db_conn
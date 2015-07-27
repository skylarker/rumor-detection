import warnings
import MySQLdb


class CreateDB(object):
    def __init__(self):
        pass

    @staticmethod
    def create_db(conn, db):
        try:
            with warnings.catch_warnings(record=True) as w:
                # Cause all warnings to always be triggered.
                warnings.simplefilter("always")
                query = 'CREATE DATABASE IF NOT EXISTS %s' % db
                conn.execute(query)
                print "Database {0} already exists!", db

        except MySQLdb.Error, e:
            print e.message
        print "Closing MySQL connection ..."
        conn.close()
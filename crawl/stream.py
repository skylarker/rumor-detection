from time import clock
import time
import sys

import MySQLdb

from tweepy import Stream, StreamListener

from model.tweet import Tweet
from model.user import User
from mysql.cursors.dict_cursor import DictCursor
from mysql.cursors.ss_cursor import SSCursor
from mysql.db.create_db import CreateDB
from mysql.executor.execute_get_dict import ExecuteGetDict
from mysql.executor.execute_write_many import ExecuteWriteMany
from mysql.select.select_col import SelectCol
from mysql.tables.construct_schema import ConstructSchema
from mysql.tables.create_table import CreateTable
from mysql.tables.insert_records import InsertRecords
from mysql.tables.read_table_records import ReadTableRecords
from twitter.auth.authenicate import OAuthenicate
from mysql.db.mysql_dal import MySQLConnector
from mysql.cursors.cursor import Cursor


start = clock()
print start


class TwitterStreamListener(StreamListener):
    def __init__(self):
        super(TwitterStreamListener, self).__init__()
        self.count = 0
        self.tweet = None
        self.user = None
        self.mysql = MySQLConnector("root", "Enig2ma11", "127.0.0.1", 3306)
        self.schema = ConstructSchema()
        self.create_db = CreateDB()
        self.get_cursor = Cursor()
        self.get_dict_cursor = DictCursor()
        self.get_ss_cursor = SSCursor()
        self.create_table = CreateTable()
        self.insert_records = InsertRecords()
        self.read_records = ReadTableRecords()
        self.select_col = SelectCol()
        self.exec_get_dict = ExecuteGetDict()
        self.exec_write_many = ExecuteWriteMany()

        self.db_conn = None
        self.db_name = 'crawler'  # MySQL Database
        self.setup_mysql()
        self.cursor = None
        self.dict_cursor = None

        self.chunk = []
        self.processed = 0

    def create_tweets_table(self):
        # construct scheme for `raw_data` table
        table_name = '`tweets`'
        cols = ['`TID`', '`TWEET`', '`CREATED_AT`', '`USER_NAME`', '`USER_SCREEN_NAME`', '`UID`', '`USER_LOCATION`',
                '`USER_TIME_ZONE`']
        types = ['VARCHAR(32)', 'TEXT', 'VARCHAR(32)', 'VARCHAR(128)', 'VARCHAR(128)', 'VARCHAR(32)', 'VARCHAR(64)',
                 'VARCHAR(32)']
        schema = self.schema.construct_schema(table_name, cols, types)
        # create table raw_data
        self.create_table.create_table(table_name, schema, self.cursor)

    def setup_mysql(self):
        # connect to MySQL
        conn = None
        try:
            conn = self.mysql.connect()
        except MySQLdb.Error, e:
            print "Failed to establish connection to MySQL server: ", e
        # get a cursor depending on the nature of executable query
        cur = self.get_cursor.cursor(conn)
        # create crawler db
        print "Creating crawler database ..."
        self.create_db.create_db(cur, self.db_name)
        # make a connection to hansya db

        try:
            self.db_conn = self.mysql.db_connect(self.db_name)
        except MySQLdb.Error, e:
            print "Failed to establish connection to  database: ", e
        self.cursor = self.get_cursor.cursor(self.db_conn)

        # get a dict cursor
        self.dict_cursor = self.get_dict_cursor.dict_cursor(self.db_conn)
        self.create_tweets_table()

    def insert_tweets(self, chunk):
        print "Inserting records into `tweets` table"
        table_name = '`tweets`'
        cols = ['`TID`', '`TWEET`', '`CREATED_AT`', '`USER_NAME`', '`USER_SCREEN_NAME`', '`UID`', '`USER_LOCATION`',
                '`USER_TIME_ZONE`']
        query = """INSERT INTO %s (""" % table_name
        cols = ', '.join(cols)
        query += cols + ') '
        query += """VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"""

        self.processed += len(chunk)
        print "Inserting tweet into `tweets` table [{0}/{1}]", self.processed, len(chunk)
        self.exec_write_many.execute_write_many("hansya", self.db_conn, query, chunk)

    def on_status(self, status):
        try:
            if status.lang == 'en':  # collect only English tweets
                self.tweet = Tweet()
                self.user = User()

                self.tweet.text = status.text.encode('utf-8').replace('\n', '\\n')
                self.tweet.id = status.id
                self.tweet.created_at = status.created_at
                self.tweet.source = status.source
                self.tweet.possibly_sensitive = status.possibly_sensitive
                self.tweet.place = status.place

                self.user.screen_name = status.author.screen_name.encode('utf-8')
                self.user.id = status.author.id
                self.user.name = status.author.name.encode('utf-8')
                self.user.location = status.author.location
                self.user.time_zone = status.author.time_zone
                self.tweet.user = self.user

                if not ('RT @' in self.tweet.text):  # Exclude re-tweets
                    """
                    pprint(vars(self.user))
                    pprint(vars(self.tweet))
                    """
                    self.count += 1
                    self.chunk.append((self.tweet.id, self.tweet.text, self.tweet.created_at, self.user.name,
                                       self.user.screen_name, self.user.id, self.user.location, self.user.time_zone))
                    if self.count % 100 == 0:
                        self.insert_tweets(self.chunk)
                        self.chunk = []

            time_pass = clock() - start
            if time_pass % 60 == 0:
                print "I have been working for", time_pass, "seconds."

        except Exception, e:
            print >> sys.stderr, 'Encountered Exception:', e
            pass

    def on_error(self, status_code):
        print 'Error: ' + repr(status_code)
        return True  # False to stop

    def on_delete(self, status_id, user_id):
        """
        Called when a delete notice arrives for a status

        """
        #print "Delete notice for %s. %s" % (status_id, user_id)
        return

    def on_limit(self, track):
        """
        Called when a limitation notice arrives

        """
        print "Limitation notice received: %s" % str(track)
        return

    def on_timeout(self):
        print >> sys.stderr, 'Timeout ...'
        time.sleep(10)
        return True


auth = OAuthenicate().authenticate()
streamTube = Stream(auth=auth, listener=TwitterStreamListener(),
                    timeout=300)
streamTube.sample()

timePass = time.clock() - start
print timePass
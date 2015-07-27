#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'Hansya Inc.'
__copyright__ = 'Copyright 2014, Hansya Inc.'
__version__ = '2.0.1'

from mysql.tables.construct_replication_query import ConstructReplicationQuery
from mysql.utils.error_handling import ErrorHandling
from mysql.db.mysql_dal import MySQLConnector
from mysql.executor.execute import Execute


class ReplicateTable(object):
    def __init__(self):
        self.error = ErrorHandling()
        self.mysql = MySQLConnector()
        self.execute = Execute()
        self.table = ConstructReplicationQuery()
        
    def replicate_table(self, db, source_table_name, destination_table_name):
        """
    
        :param db: 
        :param source_table_name: 
        :param destination_table_name: 
        """
        self.error.warn("making TABLE %s, an exact copy of TABLE %s..." % (destination_table_name, source_table_name))

        self.error.warn("connecting to DATABASE %s..." % db)
        (db_conn, db_cursor, dict_cursor) = self.mysql.db_connect(db)

        self.error.warn("cloning structure of table...")
        clone_query = self.table.construct_replication_query(db_conn, source_table_name, destination_table_name)
        self.execute.execute_(db, db_cursor, clone_query, True)

        self.error.warn("populating newly created table, TABLE %s" % destination_table_name)
        populate_query = """INSERT INTO " + destination_table_name + " SELECT * FROM """ + source_table_name
        self.execute.execute_(db, db_cursor, populate_query, True)

        self.error.warn("finished replicating table!")
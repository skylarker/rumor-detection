from mysql.db.mysql_dal import MySQLConnector
from mysql.utils.error_handling import ErrorHandling
from mysql.executor.execute_get_list_one_col import ExecuteGetListOneColumn
from mysql.executor.execute import Execute

from random import sample
from math import floor


class RandomSubset(object):
    def __init__(self):
        self.mysql = MySQLConnector()
        self.error = ErrorHandling()
        self.execute_one_col = ExecuteGetListOneColumn()
        self.execute = Execute()

    def random_subset_table(self, db, source_table_name, destination_table_name, field, percent=.10,
                            distinct=True):
        """

        This method selects a random sample of records from source table to destination table
    
        :param db: 
        :param source_table_name: 
        :param destination_table_name: 
        :param field: 
        :param percent: 
        :param distinct: 
        """
        self.error.warn("making TABLE %s, a %2.2f percent random subset of TABLE %s on unique key %s..." % (
            destination_table_name, percent, source_table_name, field))

        self.error.warn("connecting to DATABASE %s..." % db)
        (db_conn, db_cursor, dict_cursor) = self.mysql.db_connect(db)

        self.error.warn("removing destination table if it exists...")
        sql = 'DROP TABLE IF EXISTS %s' % destination_table_name
        self.execute.execute_(db, db_cursor, sql, True)

        self.error.warn("cloning structure of table...")
        sql = 'CREATE TABLE %s LIKE %s' % (destination_table_name, source_table_name)
        self.execute.execute_(db, db_cursor, sql, True)

        is_distinct_text = ' distinct' if distinct else ''
        self.error.warn('grabbing a%s subset (%2.6f percent) of the keys on which to base the new table' % (
            is_distinct_text, 100 * percent))
        sql = 'SELECT DISTINCT(%s) FROM %s' % (field, source_table_name) if distinct else 'SELECT %s FROM %s' % (
            field, source_table_name)
        unique_key_list = self.execute_one_col.execute_get_list_one_col(db, db_cursor, sql, True)

        self.error.warn(str(unique_key_list[1:5]))

        new_keys = sample(unique_key_list, int(floor(len(unique_key_list) * percent)))
        new_keys = map(str, new_keys)

        self.error.warn("populating newly created table, TABLE %s" % destination_table_name)
        populate_query = "INSERT INTO %s SELECT * FROM %s WHERE %s IN (%s)" % (
            destination_table_name, source_table_name, field, ','.join(new_keys))
        self.execute.execute_(db, db_cursor, populate_query, False)

        self.error.warn("finished making TABLE %s, a %2.2f percent random subset of TABLE %s on unique key %s!" % (
            destination_table_name, percent, source_table_name, field))
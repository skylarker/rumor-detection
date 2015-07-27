from mysql.db.mysql_dal import MySQLConnector
from mysql.executor.execute_get_list import ExecuteGetList


class GetColumnNames(object):
    def __init__(self):
        self.mysql = MySQLConnector()
        self.execute = ExecuteGetList()

    def get_table_column_names(self, db, table):
        """Returns a list of column names from a db table
        :param db: database
        :param table: table
        """
        (db_conn, db_cursor, dict_cursor) = self.mysql.db_connect(db)
        query = """SELECT column_name FROM information_schema.columns WHERE table_schema='{0:s}' AND 
                    table_name='{1:s}'""".format(db, table)
        column_names_of_table = self.execute.execute_get_list(db, db_cursor, query)
        return map(lambda x: x[0], column_names_of_table)
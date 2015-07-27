from mysql.executor.execute_get_list import ExecuteGetList
from mysql.db.mysql_dal import MySQLConnector


class GetColumnNameTypes(object):
    def __init__(self):
        self.mysql = MySQLConnector()
        self.execute = ExecuteGetList()

    def get_table_column_name_types(self, db, table):
        """
        This method returns a list of column names and types from a database table.

        :param db: database
        :param table: table

        """
        (db_conn, db_cursor, dict_cursor) = self.mysql.db_connect(db)
        query = """SELECT column_name, column_type FROM information_schema.columns WHERE table_schema='{0:s}' AND
                    table_name='{1:s}'""".format(db, table)
        return self.execute.execute_get_list(db, db_cursor, query)
from mysql.executor.execute_get_list_one_col import ExecuteGetListOneColumn
from com.hansya.mysql.db.mysql_dal import MySQLConnector


class QuickExecuteGetListOneCol(object):
    def __init__(self):
        self.mysql = MySQLConnector()
        self.execute = ExecuteGetListOneColumn()

    def quick_execute_get_list_one_col(self, db, sql, warn_query=False):
        """
        performs the db connect and execute in the same call, equivalent to execute_get_list1
        :param db:
        :param sql:
        :param warn_query:
        """
        (db_conn, db_cursor, dict_cursor) = self.mysql.db_connect(db)
        return self.execute.execute_get_list_one_col(db, db_cursor, sql, warn_query)
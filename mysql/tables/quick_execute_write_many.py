from mysql.executor.execute_write_many import ExecuteWriteMany
from mysql.db.mysql_dal import MySQLConnector


class QuickExecuteWriteMany(object):
    def __init__(self):
        self.mysql = MySQLConnector()
        self.execute = ExecuteWriteMany()

    def quick_execute_write_many(self, db, sql, rows, write_cursor=None, warn_query=False):
        """
        Executes a write query
        """
        (db_conn, db_cursor, dict_cursor) = self.mysql.db_connect(db)
        return self.execute.execute_write_many(db, db_conn, sql, rows, write_cursor, warn_query)
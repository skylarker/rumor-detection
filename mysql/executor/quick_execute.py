from mysql.db.mysql_dal import MySQLConnector
from mysql.executor.execute import Execute


class QuickExecute(object):
    def __init__(self):
        self.mysql = MySQLConnector()
        self.execute = Execute()

    def quick_execute(self, db, query, warn=False):
        """
         This method performs the db connect and execute in the same call.

        """
        (con, cursor_, dict_cursor) = self.mysql.db_connect(db)
        return self.execute.execute_(db, cursor_, query, warn)
from mysql.executor.execute_get_list import ExecuteGetList


class ExecuteGetListOneColumn(object):
    def __init__(self):
        self.execute = ExecuteGetList()

    def execute_get_list_one_col(self, db, db_cursor, sql, warn_query=False):
        """
        This method executes a given query, expecting one resulting column. Returns results as a list.

        :param db:
        :param db_cursor:
        :param sql:
        :param warn_query:
        """
        return map(lambda x: x[0], self.execute.execute_get_list(db, db_cursor, sql, warn_query))
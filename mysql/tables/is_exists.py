from mysql.executor.quick_execute_get_list_one_col import QuickExecuteGetListOneCol


class IsTableExists(object):
    def __init__(self):
        self.execute = QuickExecuteGetListOneCol()

    def does_table_exist(self, db, table):
        """

        :param db:
        :param table:
        :return:
        """
        query = 'show tables in %s' % db
        tables = self.execute.quick_execute_get_list_one_col(db, query)
        return table in tables
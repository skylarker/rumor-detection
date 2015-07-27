from mysql.executor.execute_get_dict import ExecuteGetDict


class SelectCol(object):
    def __init__(self):
        self.execute = ExecuteGetDict()

    def select_col(self, table_name, cursor, col, distinct=False):
        if distinct:
            query = """SELECT DISTINCT %s FROM %s;""" % (col, table_name)
        else:
            query = """SELECT %s FROM %s""" % (col, table_name)
        result_set = self.execute.execute_get_dict(cursor, query, True)
        return result_set
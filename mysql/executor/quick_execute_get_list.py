class QuickExecuteGetList(object):
    def __init__(self):
        pass


def quick_execute_get_list(self, db, sql, warn_query=False):
    """
    This method performs the db connect and execute in the same call.

    :param db: 
    :param sql: 
    :param warn_query: 
    """
    (db_conn, db_cursor, dict_cursor) = self.db_connect(db)
    return self.execute_get_list(db, db_cursor, sql, warn_query)
import MySQLdb


class DictCursor(object):
    def __init__(self):
        pass

    @staticmethod
    def dict_cursor(conn):
        dict_cursor = conn.cursor(MySQLdb.cursors.DictCursor)
        return dict_cursor
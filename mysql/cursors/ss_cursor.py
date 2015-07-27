import MySQLdb


class SSCursor(object):
    def __init__(self):
        pass

    @staticmethod
    def ss_cursor(conn):
        ss_cursor = conn.cursor(MySQLdb.cursors.SSCursor)
        return ss_cursor
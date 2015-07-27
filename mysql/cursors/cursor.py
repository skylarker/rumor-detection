class Cursor(object):
    def __init__(self):
        pass

    @staticmethod
    def cursor(conn):
        cursor = conn.cursor()
        return cursor
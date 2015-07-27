class ReadTableRecords(object):
    def __init__(self):
        pass

    @staticmethod
    def read_records(cursor, table_name):
        query = """SELECT * FROM %s;""" % table_name
        cursor.execute(query)
        return cursor.fetchall()
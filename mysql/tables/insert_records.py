class InsertRecords(object):
    def __init__(self):
        pass

    @staticmethod
    def insert_records(cursor, cols, values, table_name=None):
        query = """INSERT INTO %s (""" % table_name
        cols = ', '.join(cols)
        query += cols + ') '
        query += """VALUES %s""" % str(values) + ';'
        cursor.execute(query)
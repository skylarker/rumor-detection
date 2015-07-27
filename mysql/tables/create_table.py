class CreateTable(object):
    def __init__(self):
        pass

    @staticmethod
    def create_table(table_name, schema, cursor):
        query = """DROP TABLE IF EXISTS %s;""" % table_name
        cursor.execute(query)
        cursor.execute(schema)
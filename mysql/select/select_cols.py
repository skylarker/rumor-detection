class SelectCols(object):
    def __init__(self):
        pass

    @staticmethod
    def select_cols(db, table, cols, distinct=False):
        # cols list to string
        if distinct:
            query = """SELECT DISTINCT cols from db.table"""
        else:
            query = """SELECT cols from db.table"""
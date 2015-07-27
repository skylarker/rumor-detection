from mysql.tables.get_table_column_names import GetColumnNames


class GetColumnNameIndices(object):
    def __init__(self):
        self.table = GetColumnNames()

    def get_table_column_name_indices(self, db, table, col_names_of_note):
        """
        This method returns the list of column indices pertaining to the column names specified.

        :param db: database
        :param table: table
        :param col_names_of_note:
        """
        index_list = [None] * len(col_names_of_note)
        column_names_of_table = self.table.get_table_column_names(db, table)
        k = 0
        for column_name in column_names_of_table:
            i = 0
            for colOfNote in col_names_of_note:
                if column_name == colOfNote:
                    index_list[i] = k
                i += 1
            k += 1
        return index_list
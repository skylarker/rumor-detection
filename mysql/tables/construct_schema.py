import itertools


class ConstructSchema(object):
    def __init__(self):
        self.idx_cols = []

    def construct_schema(self, table_name, cols, types):
        """
        if table_name == 'unigrams_idf':
            self.idx_cols.extend(['UNIGRAM'])
        """
        query = ''
        query += """CREATE TABLE IF NOT EXISTS %s (`ID` MEDIUMINT(8) UNSIGNED NOT NULL AUTO_INCREMENT,""" % table_name
        print "Created schema for table {0}", table_name
        for col, type_ in itertools.izip_longest(cols, types):
            query += col + ' ' + type_ + ','

        for col in cols:
            if col in self.idx_cols:
                idx = col.replace('`', '') + '_IDX'
                query += ' KEY ' + '`' + idx + '`' + ' (' + col + '),'
        query += 'PRIMARY KEY (`ID`));'
        return query

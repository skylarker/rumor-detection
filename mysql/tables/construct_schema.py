import itertools


class ConstructSchema(object):
    def __init__(self):
        self.idx_cols = ['`ID`', '`DS`', '`DID`', '`SID`', '`UID`', '`BID`', '`TID`', '`FID`', '`PID`']

    def construct_schema(self, table_name, cols, types):
        if table_name == 'unigrams_idf':
            self.idx_cols.extend(['UNIGRAM'])
        if table_name == 'bigrams_idf':
            self.idx_cols.extend(['BIGRAM'])
        if table_name == 'trigrams_idf':
            self.idx_cols.extend(['TRIGRAM'])
        if table_name == 'fourgrams_idf':
            self.idx_cols.extend(['FOURGRAM'])
        if table_name == 'fivegrams_idf':
            self.idx_cols.extend(['FIVEGRAM'])
        query = ''
        query += """CREATE TABLE %s (`ID` MEDIUMINT(8) UNSIGNED NOT NULL AUTO_INCREMENT,""" % table_name
        print "Created schema for table {0}", table_name
        for col, type_ in itertools.izip_longest(cols, types):
            query += col + ' ' + type_ + ','

        for col in cols:
            if col in self.idx_cols:
                idx = col.replace('`', '') + '_IDX'
                query += ' KEY ' + '`' + idx + '`' + ' (' + col + '),'
        query += 'PRIMARY KEY (`ID`));'
        #print query
        return query
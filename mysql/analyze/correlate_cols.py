import getpass
import sys

from scipy.stats.stats import pearsonr
import numpy as np
import argparse
import MySQLdb

from mysql.db.mysql_dal import MySQLConnector


class CorrelateCols(object):
    def __init__(self):
        self.mysql = MySQLConnector()

    @staticmethod
    def _connect(db):
        user = getpass.getuser()
        conn = MySQLdb.connect(db=db, user=user)
        return conn.cursor()

    @staticmethod
    def _execute(cur, query):
        print "SQL:\t" + query[:150]
        cur.execute(query)
        return cur

    def getCols(self, cur, table, col1, col2):
        query = "select %s, %s from %s where %s is not null and %s is not null" % (col1, col2, table, col1, col2)
        cur = self._execute(cur, query)
        both = [[j for j in i] for i in cur]
        x = np.array([i[0] for i in both])
        y = np.array([i[1] for i in both])
        return x, y

    def main(self, args):
        print "Correlate two columns in a MySQL table (-h for help)"
        cur = self._connect(args.db)
        x, y = self.getCols(cur, args.table, args.col1, args.col2)
        N = len(x)

        r, p = pearsonr(x, y)

        print "\nOutcomes:        " + args.col1 + ", " + args.col2
        print "Pearson r:      %9.6f" % r
        print "p-value:        %9.6f" % p
        print "N:               %d" % N


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Correlate two columns in a MySQL table.')
    parser.add_argument('-d', '--database', dest='db',
                        help='name of the database that contains the table')
    parser.add_argument('-t', '--table', dest='table',
                        help='name of the table')
    parser.add_argument('--col1', '--column1', dest='col1',
                        help='first column')
    parser.add_argument('--col2', '--column2', dest='col2',
                        help='second column')
    args = parser.parse_args()
    if len(sys.argv) == 1:
        parser.print_help()
    else:
        main(args)

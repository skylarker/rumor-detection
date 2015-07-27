import argparse
import MySQLdb


def fetch_all_rows(cursor):
    data = cursor.fetchall()
    results = []
    for row in data:
        results.append(row[0])
    return list(set(results))


def fetch_rows_dict(cursor):
    data = cursor.fetchall()
    results = {}
    for row in data:
        c = []
        if row[0] in results:
            c = results.get(row[0])
        c.append(row[1])
        results[row[0]] = list(set(c))
    return results


def log(query):
    print "Executing query : " + query


def get_all_drop_stmts(pattern, tablename):
    query = "select concat('alter table ', table_name, ' drop column ', column_name)from INFORMATION_SCHEMA.columns where table_name = 'cdc_ageadj' and right(column_name,6) = '%s'" % args.pattern
    log(query)
    cursor.execute(query)
    return fetch_all_rows(cursor)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--pattern', dest='pattern', default=None, help='')
    parser.add_argument('--tablename', dest='tablename', default=None, help='')
    parser.add_argument('--db', dest='db')
    args = parser.parse_args()

    #Connect to db.Misuse Andy's username for now.
    db = MySQLdb.connect(host="localhost", user="root", db=args.db)
    cursor = db.cursor()
    drop = get_all_drop_stmts(args.pattern, args.tablename)
    for query in drop:
        log(query)
        cursor.execute(query)

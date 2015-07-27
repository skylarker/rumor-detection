import csv
import sys
import time
from time import mktime
from datetime import datetime

filename = sys.argv[1]

r = csv.reader(open(filename, 'rU'))

dates = []


def get_date_col(row):
    try:
        full_date = row[begin]
    except IndexError:
        return "doesn't have date"


def str_date(date):
    try:
        return time.strptime(date, "%Y-%m-%d")
    except ValueError:
        return "not a date"


begin = 5
for row in r:
    for x in xrange(begin, len(row) + 1):
        #	print row
        if get_date_col(row) == "doesn't have date":
            continue
        else:
            full_date = row[begin]
            #		print full_date
            full_date_l = full_date.split()
            #		print full_date_l
            date = full_date_l[0]
        #		print date

        if str_date(date) == "not a date":
            continue
        else:
            dates.append(str_date(date))


#print min
print datetime.fromtimestamp(mktime(min(dates)))

#print max
print max(dates)


import sys
import three
import pymongo
from pymongo import Connection

connection = Connection()
db = connection.open311.requests
endpoint_city = 'baltimore'

city = three.city(endpoint_city)

for r in range(5):
    p = r + 1
    print "downloading request data {0}, page {1}...".format(endpoint_city, p)
    requests = city.requests(between=['01-01-2012', '04-01-2012'], page_size=1000, page=p)
    print "download complete"
    
    print "inserting downloaded data into db:"
    for request in requests:
        sys.stdout.write('.')
        sys.stdout.flush()
        db.insert(request)
    print "database insert complete"

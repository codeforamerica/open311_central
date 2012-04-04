import os, sys, pymongo
from pymongo import Connection
from optparse import OptionParser
import iso8601

endpoint_city = 'baltimore' # TODO: replace this with command line arg
connection = Connection(os.environ['MONGO_URI'])
db = connection[os.environ['MONGO_COLLECTION']]

print "adding date field to all requests"
for doc in db.requests.find():
    date = iso8601.parse_date(doc['requested_datetime'])
    doc['date'] = date.strftime('%Y-%m-%d')
    db.requests.save(doc)

print "creating index on date field"
db.requests.create_index("date", unique=False)

print "done"

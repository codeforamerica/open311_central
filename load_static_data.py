import os, sys, three, pymongo
from pymongo import Connection

endpoint_city = 'baltimore' # TODO: replace this with command line arg
connection = Connection(os.environ['MONGO_URI'])
db = connection.chicago

print "destroy previous static tables"
db.drop_collection('services')

print "downloading static data from {0}...".format(endpoint_city)
city = three.city(endpoint_city)
services = city.services()
print "download complete"

print "inserting downloaded data into db:"
for service in services:
    sys.stdout.write('.')
    sys.stdout.flush()
    db.services.insert(service)
print "\ndatabase insert complete"

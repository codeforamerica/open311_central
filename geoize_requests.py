import os, sys, pymongo
from pymongo import Connection, GEO2D
from optparse import OptionParser
from pymongo.errors import OperationFailure

endpoint_city = 'baltimore' # TODO: replace this with command line arg
connection = Connection(os.environ['MONGO_URI'])
db = connection[os.environ['MONGO_COLLECTION']]

print "updating requests with geo data for {0}".format(endpoint_city)

for neighborhood in db.baltimore_neighborhoods.find():
    try:
        neighborhood_name = neighborhood['properties']['Name']
        print "doing updates for {0}".format(neighborhood_name) 
        coordinates = neighborhood['geometry']['coordinates']
    except KeyError as ke:
        print "could not process shape {0}".format(neighborhood)
        continue
    try:
        docs = db.requests.find({"loc": {"$within": {"$polygon": coordinates[0]}}})
        for doc in docs:
            doc['neighborhood'] = neighborhood_name 
            db.requests.save(doc)
    except OperationFailure as of:
        print "couldn't do geospatial search " \
            "for polygon lookup for {0}".format(neighborhood_name)
        print "reason: {0}".format(of)
    except Exception as e:
        print "couldn't do geospatial search " \
            "for polygon lookup for {0}".format(neighborhood_name)
        print "reason: {0}".format(e)


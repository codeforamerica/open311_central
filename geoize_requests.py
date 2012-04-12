import os, sys, pymongo, argparse
from pymongo import Connection, GEO2D
from pymongo.errors import OperationFailure

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Stamp boundary name on all requests ' +
        'in MongoDB')
    args = parser.parse_args()
    connection = Connection(os.environ['MONGO_URI'])
    db = connection[os.environ['MONGO_DATABASE']]

    print "updating requests with boundary name"

    for boundary in db.boundaries.find():
        try:
            boundary_name = boundary['properties']['Name']
            print "doing updates for {0}".format(boundary_name) 
            coordinates = boundary['geometry']['coordinates']
        except KeyError as ke:
            print "could not process shape {0}".format(boundary)
            continue
        try:
            docs = db.requests.find({"loc": {"$within": {"$polygon": coordinates[0]}}})
            for doc in docs:
                doc['boundary'] = boundary_name
                db.requests.save(doc)
        except OperationFailure as of:
            print "couldn't do geospatial search " \
                "for polygon lookup for {0}".format(boundary_name)
            print "reason: {0}".format(of)
        except Exception as e:
            print "couldn't do geospatial search " \
                "for polygon lookup for {0}".format(boundary)
            print "reason: {0}".format(e)


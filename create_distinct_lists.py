import os, sys, pymongo, iso8601, argparse
from bson.code import Code
from pymongo import Connection
from optparse import OptionParser
from datetime import datetime, timedelta, date

def create_distinct_lists_of_boundaries_and_request_types_for_endpoints():
    boundaries = db.requests.find({"endpoint": endpoint}).distinct('boundary');
    db.distinct.save({"_id": endpoint, "boundaries": boundaries})

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Create table of distinct lists ' +
        'for each endpoint and store in MongoDB')
    parser.add_argument('endpoint', action='store', help='three city name')
    args = parser.parse_args()
    endpoint = args.endpoint
    connection = Connection(os.environ['MONGO_URI'])
    db = connection[os.environ['MONGO_DATABASE']]

    print 'creating distinct list of boundaries for {0}'.format(endpoint)
    create_distinct_lists_of_boundaries_and_request_types_for_endpoints()
    print 'done'
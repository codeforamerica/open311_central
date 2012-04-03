import os, sys, pymongo
from bson.code import Code
from pymongo import Connection
from optparse import OptionParser

def sum_requests_by_status():
    map = Code("function() {"
               "  emit(this.status, {count: 1});"
               "}")

    reduce = Code("function(k, v) {"
                  "  var result = {count: 0};"
                  "  v.forEach(function(value) {"
                  "    result.count += value.count;"
                  "  });"
                  "  return result;"
                  "}")

    db.requests.map_reduce(map, reduce, "request_status_sum")
    print "summed requests by status"

def sum_requests_by_neighborhood():
    map = Code("function() {"
               "  emit(this.status, {count: 1});"
               "}")

    reduce = Code("function(k, v) {"
                  "  var result = {count: 0};"
                  "  v.forEach(function(value) {"
                  "    result.count += value.count;"
                  "  });"
                  "  return result;"
                  "}")

    db.requests.map_reduce(map, reduce, "request_status_sum")
    print "summed requests by status"

if __name__ == '__main__':
    parser = OptionParser()
    #parser.add_option('-e', '--endpoint', dest='endpoint', help='The three endpoint to query')

    endpoint_city = 'baltimore' # TODO: replace this with command line arg
    connection = Connection(os.environ['MONGO_URI'])
    db = connection[os.environ['MONGO_COLLECTION']]

    # TODO: remove this after incremental map/reduce is implemented
    print "dropping previous mapreduce results tables"
    #db.drop_collection('requests')
    
    sum_requests_by_status()


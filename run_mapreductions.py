import os, sys, pymongo, iso8601
from bson.code import Code
from pymongo import Connection
from optparse import OptionParser
from datetime import datetime, timedelta, date

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
               "  emit(this.neighborhood, {count: 1});"
               "}")
    reduce = Code("function(k, v) {"
                  "  var result = {count: 0};"
                  "  v.forEach(function(value) {"
                  "    result.count += value.count;"
                  "  });"
                  "  return result;"
                  "}")
    db.requests.map_reduce(map, reduce, 'request_neighborhood_open_sum', \
        query={'status':'open'})
    print "summed open requests by neighborhood"
    db.requests.map_reduce(map, reduce, 'request_neighborhood_closed_sum', \
        query={'status':'closed'})
    print "summed closed requests by neighborhood"

def _upsert_date_and_increment_open_count(date_hash, service_name, date):
    if date not in date_hash:
        type_hash = {service_name: {"open": 1}}
        date_hash[date] = {"open": 1, "types": type_hash} 
        return
    date_hash[date]["open"] += 1 
    type_hash = date_hash[date]['types']
    if service_name not in type_hash:
        type_hash[service_name] = {"open": 1}
        return
    type_hash[service_name]["open"] += 1

def sum_open_requests_by_date():
    date_hash = {}
    print "summing open requests by date"
    open_requests = \
        db.requests.find({"status": "open"})
    for document in open_requests:
        created_at = iso8601.parse_date(document['requested_datetime']).date()
        delta = date.today() - created_at
        for i in range(0, delta.days+1):
            _upsert_date_and_increment_open_count(date_hash, document['service_name'], \
                created_at + timedelta(i))
    closed_requests = \
        db.requests.find({"status": "closed"})
    for document in closed_requests:
        created_at = iso8601.parse_date(document['requested_datetime']).date()
        updated_at = iso8601.parse_date(document['updated_datetime']).date()
        delta = updated_at - created_at
        for i in range(0, delta.days):
            _upsert_date_and_increment_open_count(date_hash, document['service_name'], \
                created_at + timedelta(i))
    print "saving open requests by date" 
    db.requests_open_by_date.remove() 
    sorted_h = sorted(date_hash)
    print "sorted_h = {0}".format(sorted_h)
    for v in sorted_h:
        print v
        db.requests_open_by_date.insert( { "date": v.strftime('%Y-%m-%d'), "counts": date_hash[v]} )
    db.requests_open_by_date.ensure_index([("date", pymongo.ASCENDING)], unique=False)
    
if __name__ == '__main__':
    endpoint_city = 'baltimore' # TODO: replace this with command line arg
    connection = Connection(os.environ['MONGO_URI'])
    db = connection[os.environ['MONGO_COLLECTION']]
    
    sum_requests_by_status()
    sum_requests_by_neighborhood()
    sum_open_requests_by_date()

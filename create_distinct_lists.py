import os, sys, pymongo, iso8601, argparse
from bson.code import Code
from pymongo import Connection
from optparse import OptionParser
from datetime import datetime, timedelta, date

def create_distinct_lists_of_boundaries_and_request_types_for_endpoints():
    boundaries = [] 
    boundaryNames = db.requests.find({"endpoint": endpoint}).distinct('boundary')
    for boundaryName in boundaryNames:
        boundary = db.boundaries.find_one({"properties.Name": boundaryName})
        lats = []
        lons = []
        for coordinates in boundary["geometry"]["coordinates"]:
            for lat,lon in coordinates:
                lats.append(lat)
                lons.append(lon)
        bbox = [min(lats), min(lons), max(lats), max(lons)]
        record = {"name": boundaryName, "boundary": bbox}
        boundaries.append(record)
        print record
    
    # service name/codes 
    service_info = []
    service_names = db.requests.find({"endpoint": endpoint}).distinct('service_name')
    for name in service_names:
        result = db.services.find_one({"endpoint": endpoint, "service_name": name},
                                      fields={"service_code": 1})
        if None == result:
            continue
        service_info.append({"service_name": name, 
                             "service_code": result["service_code"]})

    row = {"_id": endpoint, "boundaries": boundaries, "services": service_info}
    db.distinct.save(row)
        
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

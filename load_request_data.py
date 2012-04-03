import os, sys, three, pymongo
from pymongo import Connection, GEO2D
from optparse import OptionParser

def download_requests():
    requests_remain = True
    page_number = 1
    while(requests_remain):
        print "downloading request data {0}, page {1}...".format(endpoint_city, 
           page_number)
        requests = city.requests(page_size=100, page=page_number)
        print "download complete"
        print "inserting downloaded data into db:"
        for request in requests:
            sys.stdout.write('.')
            sys.stdout.flush()
            request['loc'] = [request['long'], request['lat']]
            db.requests.insert(request)
        page_number = page_number + 1
        requests_remain = len(requests) > 0

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('-e', '--endpoint', dest='endpoint', help='The three endpoint to query')

    endpoint_city = 'baltimore' # TODO: replace this with command line arg
    connection = Connection(os.environ['MONGO_URI'])
    db = connection[os.environ['MONGO_COLLECTION']]

    # TODO: remove this after incremental update start happening
    print "dropping requests table"
    db.drop_collection('requests')
    
    city = three.city(endpoint_city)
    print "downloading requests from {0}...".format(endpoint_city)
    download_requests()

    print "setting geospatial index on loc field"
    db.requests.ensure_index([("loc", GEO2D)])


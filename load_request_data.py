import os, sys, three, pymongo, argparse
from pymongo import Connection, GEO2D

def download_requests():
    requests_remain = True
    page_number = 1
    while(requests_remain):
        print 'downloading request data {0}, page {1}...'.format(endpoint, 
           page_number)
        requests = city.requests(page_size=100, page=page_number)
        print 'download complete'
        print 'inserting downloaded data into db:'
        for request in requests:
            try:
                sys.stdout.write('.')
                sys.stdout.flush()
                request['endpoint'] = endpoint
                request['loc'] = [request['long'], request['lat']]
                request['_id'] = '{0}.{1}'.format(endpoint, request['token'])
                db.requests.save(request)
            except KeyError as ke:
                print 'could not insert: {0}'.format(ke)
                print request
        print 'done'
        page_number = page_number + 1
        requests_remain = len(requests) > 0

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Download Open311 data from endpoint ' +
        'and store in MongoDB')
    parser.add_argument('endpoint', action='store', 
                        help='three city name')
    args = parser.parse_args()
    endpoint = args.endpoint
    connection = Connection(os.environ['MONGO_URI'])
    db = connection[os.environ['MONGO_DATABASE']]

    print 'downloading requests from {0}...'.format(endpoint)
    city = three.city(endpoint)
    download_requests()

    print 'setting geospatial index on loc field'
    db.requests.ensure_index([("loc", GEO2D)])

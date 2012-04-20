import os, sys, three, pymongo, argparse
from pymongo import Connection, GEO2D
from log_manager import LogManager

def download_requests():
    requests_remain = True
    page_number = 1
    while(requests_remain):
        logger.info('downloading request data {0}, page {1}...'
              .format(endpoint, page_number))
        requests = city.requests(page_size=100, page=page_number)
        print 'inserting page {0} for {1}:'.format(page_number, endpoint)
        for request in requests:
            try:
                request['endpoint'] = endpoint
                request['loc'] = [request['long'], request['lat']]
                request['_id'] = '{0}.{1}'.format(endpoint, request['token'])
                db.requests.save(request)
            except KeyError as ke:
                logger.error('could not insert: {0}'.format(ke))
                logger.error(request)
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
    lm = LogManager()
    logger = lm.logger

    logger.info('downloading requests from {0}...'.format(endpoint))
    city = three.city(endpoint)
    download_requests()

    logger.info('setting geospatial index on loc field')
    db.requests.ensure_index([("loc", GEO2D)])


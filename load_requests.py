import os, sys, three, pymongo, argparse
from pymongo import Connection, GEO2D
from pymongo.errors import OperationFailure
from log_manager import LogManager
from datetime import datetime
from time import sleep

def download_requests():
    requests_remain = True
    page_number = 1
    global processed_requests
    while(requests_remain):
        logger.info('downloading request data {0}, page {1}...'
              .format(endpoint, page_number))
        requests = city.requests(page_size=1000, page=page_number)
        logger.info('inserting page {0} for {1}:'.format(page_number, endpoint))
        for request in requests:
            try:
                request['endpoint'] = endpoint
                request['loc'] = [request['long'], request['lat']]
                request['_id'] = '{0}.{1}'.format(endpoint, request['token'])
                db.requests.save(request)
                processed_requests = processed_requests + 1
            except KeyError as ke:
                logger.error('could not insert: {0}'.format(ke))
                logger.error(request)
        page_number = page_number + 1
        requests_remain = len(requests) > 0

def mark_requests_with_boundaries():
    for boundary in db.boundaries.find():
        try:
            boundary_name = boundary['properties']['Name']
            logger.info("doing request boundary updates for {0}".format(boundary_name))
            coordinates = boundary['geometry']['coordinates']
        except KeyError as ke:
            logger.error("could not process shape {0}".format(boundary))
            continue
        try:
            docs = db.requests.find({"loc": {"$within": {"$polygon": coordinates[0]}}})
            for doc in docs:
                doc['boundary'] = boundary_name
                db.requests.save(doc)
        except OperationFailure as of:
            logger.error("couldn't do geospatial search " \
                "for polygon lookup for {0}".format(boundary_name))
        except Exception as e:
            logger.error("couldn't do geospatial search " \
                "for polygon lookup for {0}".format(boundary))

def increment_running_count_in_stats():
    script_stats = db.scriptstats.find_one({'_id': endpoint}) or {}
    script_stats['_id'] = endpoint
    if 'running_count' in script_stats:
        script_stats['running_count'] = script_stats['running_count'] + 1 
    else:
        script_stats['running_count'] = 1
    script_stats['last_started_at'] = datetime.now().strftime('%A, %d. %B %Y %H:%M:%S')
    logger.debug(script_stats)
    db.scriptstats.save(script_stats)
    return script_stats

def update_stats(script_stats):
    script_stats['running_count'] = script_stats['running_count'] - 1
    script_stats['processed_requests'] = processed_requests 
    script_stats['last_completed_at'] = datetime.now().strftime('%A, %d. %B %Y %H:%M:%S')
    db.scriptstats.save(script_stats)

if __name__ == '__main__':
    endpoints = ['baltimore', 'boston']
    lm = LogManager()
    logger = lm.logger
    connection = Connection(os.environ['MONGO_URI'])
    db = connection[os.environ['MONGO_DATABASE']]
    while(True):
        for endpoint in endpoints:
            processed_requests = 0
            script_stats = increment_running_count_in_stats()
            logger.info('downloading requests from {0}...'.format(endpoint))
            city = three.city(endpoint)
            download_requests()
            logger.info('setting geospatial index on loc field')
            db.requests.ensure_index([("loc", GEO2D)])
            mark_requests_with_boundaries() 
            update_stats(script_stats)
        logger.debug('requests downloaded; sleeping for 1 hour')
        sleep(3600)

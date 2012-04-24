import os, sys, three, pymongo, argparse
from pymongo import Connection
from pymongo.errors import OperationFailure
from log_manager import LogManager
from datetime import datetime

def mark_requests_with_boundaries():
    """ 
        Gather all the open requests for an endpoint, put them in an array 
        and then save as a document with key = endpoint, value = array,
        save it all in mongo openrequests collection 
    """
    result = {'_id': endpoint}
    open_requests = []
    for request in db.requests.find({'status': 'open', 'endpoint': endpoint}):
        open_requests.append(request)
    result['open_requests'] = open_requests
    print len(open_requests)
    db.openrequests.save(result)

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

    mark_requests_with_boundaries()

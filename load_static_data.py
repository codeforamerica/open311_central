import os, sys, three, pymongo, argparse
erom pymongo import Connection
from log_manager import LogManager

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Download Open311 service data ' +
        'and store in MongoDB')
    parser.add_argument('endpoint', action='store',
                        help='three city name')
    args = parser.parse_args()
    endpoint = args.endpoint
    connection = Connection(os.environ['MONGO_URI'])
    db = connection[os.environ['MONGO_DATABASE']]
    lm = LogManager()
    logger = lm.logger

    logger.info("downloading static data from {0}...".format(endpoint))
    city = three.city(endpoint)
    services = city.services()
    logger.info("download of static data complete for {0}".format(endpoint))

    for service in services:
        service['_id'] = service['service_code']
        service['endpoint'] = endpoint
        db.services.save(service)

    logger.info("saved static data for {0}".format(endpoint))

import os, sys, three, pymongo, argparse
from pymongo import Connection

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Download Open311 service data ' +
        'and store in MongoDB')
    parser.add_argument('endpoint', action='store',
                        help='three city name')
    args = parser.parse_args()
    endpoint = args.endpoint
    connection = Connection(os.environ['MONGO_URI'])
    db = connection[os.environ['MONGO_DATABASE']]

    print "downloading static data from {0}...".format(endpoint)
    city = three.city(endpoint)
    services = city.services()
    print "download complete"

    print "inserting downloaded data into db:"
    for service in services:
        sys.stdout.write('.')
        sys.stdout.flush()
        service['_id'] = service['service_code']
        service['endpoint'] = endpoint
        db.services.save(service)
    print "\ndatabase insert complete"

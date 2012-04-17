import os
from pymongo import Connection

connection = Connection(os.environ['MONGO_URI'])
db = connection[os.environ['MONGO_DATABASE']] 

total = db.requests.find().count()
total_baltimore = db.requests.find({'endpoint': 'baltimore'}).count()
total_boston = db.requests.find({'endpoint': 'boston'}).count()
total_boston_no_boundary = db.requests.find({'endpoint': 'boston',
                                             'boundary': {'$exists': False}}).count()
total_baltimore_no_boundary = db.requests.find({'endpoint': 'baltimore',
                                                'boundary': {'$exists': False}}).count()

print 'total requests: {0}'.format(total)
print 'total baltimore requests: {0}'.format(total_baltimore)
print 'total boston requests: {0}'.format(total_boston)
print 'total baltimore requests no boundary: {0}'.format(total_baltimore_no_boundary)
print 'total boston requests no boundary: {0}'.format(total_boston_no_boundary)

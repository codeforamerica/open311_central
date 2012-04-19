import os, logging
from pymongo import Connection
from log4mongo.handlers import MongoHandler

class LogManager:
    def __init__(self):
        self.logger = logging.getLogger('logman')
        self.logger.setLevel(logging.DEBUG)
        self.logger.addHandler(MongoHandler(host=os.environ['MONGO_URI'],
                               database_name=os.environ['MONGO_DATABASE']))

        # create console handler and set level to debug
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        
        # create formatter
        formatter = \
            logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        # add formatter to ch
        ch.setFormatter(formatter)
        
        # add ch to logger
        self.logger.addHandler(ch)

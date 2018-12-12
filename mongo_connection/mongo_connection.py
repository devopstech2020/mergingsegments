from sys import exit
from time import time
from pymongo import MongoClient, DESCENDING, ASCENDING
from pymongo.errors import ConnectionFailure
from bson.json_util import dumps
from json import loads
from bson.objectid import ObjectId


class database():
    """constructor method"""
    def __init__(self, logger, database, collection, **kwargs):
        self.kwargs = kwargs
        self.logger = logger
        self.collection = collection
        self.database = database

        ##connecting with mongo
        try:
            if self.kwargs["password_auth"] == True:
                self.conn = MongoClient(host=self.kwargs["host"], port=self.kwargs["port"], username=self.kwargs["username"], password=self.kwargs["password"],authSource=self.kwargs["auth_db"])
            else:
                self.conn = MongoClient(host=self.kwargs["host"], port=self.kwargs["port"])
            self.logger.info("Connected sucessfully to mongodb")
        except ConnectionFailure as e:
            self.logger.warn("Could not connect to MonogoDB: {}".format(e))
            exit(1)

        ##creating collection object
        try:
            self.dbh = self.conn[self.database]
            self.collection_obj = self.dbh[self.collection]
            self.logger.info("Sucessfully connected to collection {}  ".format(self.collection))

        except Exception as e:
            self.logger.warn("Could not connect to collection {} {}".format(self.database, e))

    """method to show of collection in db"""
    def return_collections(self):
        try:
            return self.dbh.collection_names()

        except Exception as e:
            self.logger.warn("Error: {}".format(e))

    """select one document by id"""
    def find_one_doc(self, fieldName, fieldValue):
        try:
            if fieldName == "_id":
                searchString = {
                    fieldName: ObjectId(fieldValue)
                }
            else:
                searchString = {
                    fieldName: fieldValue
                }

            data = self.collection_obj.find(searchString).sort('_id', DESCENDING).limit(1)
            return loads(dumps(data))
        except Exception as e:
            self.logger.warn("problem selecting row from database with fieldname {} {}".format(fieldName, e))

    """select one document by id"""

    def find(self):
        try:
            cursor = self.collection_obj.find()
            return cursor
        except Exception as e:
            self.logger.warn("problem selecting row from database with fieldname {}".format(e))

    def insert_many(self, data):
        try:
            self.collection_obj.create_index("default_domain", background=True)
            self.collection_obj.create_index("email_id", background=True)
            self.collection_obj.insert_many(data)
            return True
        except Exception as e:
            self.logger.warn("Problem inserting many data {}".format(e))



    """method to find rows"""
    def find_rows(self, pipeline):
        data =  self.collection_obj.aggregate(pipeline)
        return loads(dumps(data))

    """method to return lastest row"""
    def latest_row(self, field):
        return loads(dumps(self.collection_obj.find().sort(field, DESCENDING).limit(1)))


    """method to insert row"""
    def insert_one_doc(self,doc):
        try:
            self.collection_obj.insert_one(doc)
            return True
        except Exception as e:
            self.logger.warn("there is problem writting document{}".format(e))

    """method to update rows update rows"""
    def update_collection(self, auto_incre_id):
        try:
            self.collection_obj.update_one({
                'name': 'segment_master'
                },
                {'$set' : {
                      "lastcount" : auto_incre_id
                }})
            return True
        except Exception as e:
            self.logger.warning("problem update segment master {}".format(e))


    """method to close mongo connection"""
    def close(conn, logger):
        conn.close()
        logger.info("connection to mongodb has been closed")

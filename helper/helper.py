from sys import exit
from datetime import datetime
import time
import pandas as pd

"""class to get segment details"""
class fetch_seg_detail:
    def __init__(self, id, db, logger):
        self.id = id
        self.db = db
        self.logger = logger
        try:
            self.doc = self.db.find_one_doc("_id", self.id)
        except Exception as e:
            self.logger("Error: feching document {}".format(e))

    def collection_name(self):
        return self.doc[0]['segment_collection_name']

    def segment_name(self):
        return self.doc[0]['segment_name']

    def segment_id(self):
        return self.doc[0]['_id']['$oid']

    def empty(self):
        if self.doc is None:
            return False
        return True

"""class for checking segment master"""
class segment_master:
    def __init__(self, db, logger, merge_id):
        self.db = db
        self.logger = logger
        self.merge_id = merge_id
        try:
            self.doc = self.db.find_one_doc("merge_id", self.merge_id)
        except Exception as e:
            self.logger("Error: feching document {}".format(e))

    def segment_time(self):
        created_date =  self.doc[0]["created_date"]
        return str(datetime.fromtimestamp(created_date['$date']/1000).strftime("%Y-%m-%d"))

    def today_date(self):
        return  str(datetime.now().date())


    def return_id(self):
        return self.doc[0]['_id']['$oid']

    def if_segment_exist(self):
        if len(self.doc) > 0:
            if self.today_date() == self.segment_time():
                return True
        return False


class sequence_master:
    def __init__(self, mongo_obj, logger):
        try:
            self.doc = mongo_obj.find_one_doc("name", "segment_master")
        except Exception as e:
            self.logger("Error: feching document {}".format(e))

    def last_autoincrenetid(self):
        return self.doc[0]['lastcount']




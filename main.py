#!/usr/bin/python3

"""main function for the script"""
def main():
    #current working dir
    current_dir = Path('/home/devinder/nxtGenJS')

    #logs dir
    log_file = Path('/var/log/pythonscript.log')

    #config file
    config_file = current_dir.joinpath('cfg/config.cfg')
    #making logger objectes
    logger = log_script.log_script(log_file, "main")
    helper_logger = log_script.log_script(log_file, "helper")
    mongo_logger = log_script.log_script(log_file, "mongo")

    #arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('files', metavar='F', type=str, nargs='+', help="Enter strings")
    args = parser.parse_args()

    
    """function to load config file"""
    def load_config(file):
        try:
#            if file.exists():
            config = json.loads(file.open("r").read())
            return config
#            else:
#                logger.warn("Config file does not exist")
        except Exception as e:
            logger.warn("Problem loading config file Error: {}".format(e))

    config = load_config(config_file)
     
     #####variables to be passed to mongo db for connection
    data = {
        "host" : config["mongo_config"]["host"],
        "port" : config["mongo_config"]["port"],
        "username" : config["mongo_config"]["username"],
        "password" : config["mongo_config"]["password"],
        "auth_db" : config["mongo_config"]["auth_db"],
        "password_auth" : config["mongo_config"]["password_auth"]
    }
    #####mongo db database connections
    seg_master = mongo_connection.database(mongo_logger, config["mongo_config"]["db"], config["mongo_config"]["master_table"] , **data)
    seq_master = mongo_connection.database(mongo_logger, config["mongo_config"]["db"], "sequence_master", **data )

    seg_objs = []
    segment_collection_names = []
    arguments = list(set(args.files))
    for id in arguments:
        seg_obj = helper.fetch_seg_detail(id, seg_master, helper_logger)
        if seg_obj.empty() is not False:
            seg_objs.append(seg_obj)

    new_merged_id  = ",".join(sorted([obj.segment_id() for obj in seg_objs]))
    [segment_collection_names.append(obj.collection_name()) for obj in seg_objs]


    segment_master  = helper.segment_master(seg_master, helper_logger, new_merged_id)

    if not seg_objs:
        logger.warn("Not segment details found")
        exit(1)
    elif len(seg_objs) > 1:
        if segment_master.if_segment_exist() is True:
            print(segment_master.return_id())
            exit(0) 
    else:
        print(seg_objs[0].segment_id())
        exit(0)


    def create_cursor(collection_name):
        cursors = []
        try:
            cursor_obj = mongo_connection.database(mongo_logger, config["mongo_config"]["db"], collection_name, **data)
            return cursor_obj.find()

        except Exception as e:
            logger.warn("problem creating cursor")

    cursors = [pd.DataFrame(list(create_cursor(collection))) for collection in segment_collection_names]

    def insert_data(func):
        def wrapper(data_frame):
            try:
                seg_name = "seg_" + str(time.time()).split(".")[0]
                data_frame = func(data_frame)
                rows, columns = data_frame.shape
                json_data = json.loads(data_frame.to_json(orient='records'))
                obj = mongo_connection.database(mongo_logger, config["mongo_config"]["db"], seg_name, **data)
                if (obj.insert_many(json_data)):
                    logger.info("New segment sucessfully inserted into collection {}".format(seg_name))
                    del json_data
                    return (seg_name, rows)

            except Exception as e:
                logger.info("problem creating json_data: {}".format(e))
        return wrapper

    @insert_data
    def create_pd(cursor):
        try:
            data = pd.concat(cursor, axis=0)
            data1 = data.drop(['_id'], axis=1 )
            x,y = data1.shape
            if x == 0 and y == 0:
                  logger.info("count is zero")
                  print("count is zero")
                  exit(0)		
            return data1.drop_duplicates('email_id')
        except  Exception as e:
            logger.warn("problem creating pandas : {}".format(e))

    new_segment_name, count = create_pd(cursors)
    merged_segment_name = ",".join([obj.segment_name() for obj in seg_objs])
    merged_collection_name = ",".join([obj.collection_name() for obj in seg_objs])


    seq = helper.sequence_master(seq_master, helper_logger)
    last_id = seq.last_autoincrenetid()
    date = datetime.datetime.fromtimestamp(time.time())
    
    if seg_master.insert_one_doc({
        'segment_collection_name' : new_segment_name,
   		'_class' : "com.digi.mongo.model.SegmentMaster",
		'autoincrement_id' :  last_id + 1,
		'segment_name' : merged_collection_name,
		'merge_id' : new_merged_id,
		'count' : count,
		'description' : "It also opens up the possibility of vertical alignment with the 'alig...",
		'last_used' : date,
		'last_generated' : date,
		'created_date'  :  date,
		'is_active' : True,
        'type' : "mixed"  }):

        ###update sequence master
        seq_master.update_collection(last_id+1)
        row = seg_master.find_one_doc('segment_collection_name', new_segment_name)
       
        if row:
             sid = row[0]['_id']['$oid']
             logger.info("\"{}\"".format(sid))
             print (sid)
             exit(0)        
        else:
             print("Error")
        # segment_master1  = helper.segment_master(seg_master, helper_logger, new_merged_id )
        #if segment_master1.if_segment_exist() is True:
        #    logger.info(type(segment_master1.return_id()))
        #    sid = segment_master1.return_id()
        #    print(sid.strip()) 
if __name__ ==  "__main__":
    import argparse
    from pathlib import Path
    from json import loads
    import time
    from sys import exit
    import json
    import datetime
    from log_script import log_script
    from helper import helper
    from mongo_connection import mongo_connection
    import pandas as pd
    main()
    
    

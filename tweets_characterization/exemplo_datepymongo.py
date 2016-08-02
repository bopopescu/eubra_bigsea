#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
Example of Mongo connection, Mongo query
to retrieve tweets and operations using the 
json package.
'''

''' IMPORT PACKAGES '''
import sys
# Mongo DB management
import pymongo
# Dealing with json
import json
# Dealing with date and time
import datetime
# from datetime import datetime 

import time
# Parsing command line parameters
import argparse
import re

from filters import *

from bson import Binary, Code
from bson.json_util import dumps

from pytz import timezone
import pytz

import dateutil.parser


# THIS CODE ENSURE THE CORRECT ENCODING (OPTIONAL)
reload(sys)
sys.setdefaultencoding('utf-8')



''' REMOVI EMOJIS AND SYMBOLS'''
emoji_pattern = re.compile("["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                           "]+", flags=re.UNICODE)



''' GETS THE COMMAND LINE PARAMETERS '''
def get_parameters():
    global args
    parser = argparse.ArgumentParser(description='This program retrieve from MongoDB and deal with json tweets.')
    parser.add_argument('-s','--server', help='Name of the MongoDB server', required=True)
    parser.add_argument('-p','--persistence', help='Name of the MongoDB persistence slave', required=False)
    parser.add_argument('-d','--database', help='Name of the MongoDB database', required=True)
    parser.add_argument('-c','--collection', help='Name of the MongoDB collection', required=True)
    parser.add_argument('-sd','--startDate', help='The date when a project or task is scheduled to begin/start. Ex: "AAAA-MM-DD HH:MM:SS" (ISO-8601)', required=True)
    parser.add_argument('-ed','--endDate', help='The date when a project or task is scheduled to finish/end. Ex: "AAAA-MM-DD HH:MM:SS" (ISO-8601)', required=True)
    parser.add_argument('-o','--output', help='Name of the output file', required=True)
    args = vars(parser.parse_args())



''' OPENS THE MONGO DB CONNECTION '''
def connect_db():
    global args, client
    ERROR = True #Controls if occurs connection error
    count_attempts = 0 #Number of attempts for connection
    while ERROR:
        try:
            count_attempts += 1
            # OPEN THE CONNECTION WITH THE MONGO DB
            if (args['persistence'] == None): client = pymongo.MongoClient(args['server'])
            else: client = pymongo.MongoClient([args['server'], args['persistence']])
            client.server_info()
            print 'MongoDB Connection opened after', str(count_attempts), 'attempts', str(datetime.datetime.now())
            ERROR = False
        except pymongo.errors.ServerSelectionTimeoutError:
            print 'MongoDB connection failed after', str(count_attempts), 'attempts. ', \
                  str(datetime.datetime.now()), '. A new attempt will be made in 60 seconds'
            ERROR = True
            time.sleep(60)
        if ( count_attempts > 20 ):
           print 'It was not possible to connect to MongoDB. Shutting down...'
           sys.exit()

'''FUNCTION TO RETRIEVE THE TWEETS BETWEEN TWO SPECIFIC DATES'''
def retrieve_data_from_dates(collection, startDate,endDate):
	# run correct
	print startDate
	print endDate
	return collection.find({'created_at': {'$gte': startDate, '$lte': endDate }}, no_cursor_timeout=False)
	#return collection.find({'created_at': (startDate + datetime.timedelta(hours=3))}, no_cursor_timeout=False)

	# return collection.find({'created_at': datetime.datetime(2016,03,30,01,00,00)}, no_cursor_timeout=False)


    #return collection.find({'created_at': datetime.datetime(2016,03,22,17,27,28)}).sort([['_id', pymongo.DESCENDING]]).limit(10)
    #return collection.find().sort([['_id', pymongo.DESCENDING]]).limit(10)
    #return collection.find().sort([['created_at', pymongo.DESCENDING]]).limit(10)
    #return collection.find({'created_at': {'$gte': datetime.datetime(2016,07,28,00,00,00), '$lte': datetime.datetime(2016,07,28,23,59,59)}}).sort([['created_at', pymongo.DESCENDING]]).limit(10)
	
	# datas passadas por parametros
	# return collection.find({'created_at': {'$gte': datetime.datetime(2016,07,28,00,00,00), '$lte': datetime.datetime(2016,07,28,23,59,59)}},no_cursor_timeout=False).sort([['created_at', pymongo.DESCENDING]])
	


'''FUNCTION TO RETRIEVE THE TWEETS'''
def retrieve_data(collection):
	# run correct
    #return collection.find({'created_at': datetime.datetime(2016,03,22,17,27,28)}).sort([['_id', pymongo.DESCENDING]]).limit(10)
    #return collection.find().sort([['_id', pymongo.DESCENDING]]).limit(10)
    #return collection.find().sort([['created_at', pymongo.DESCENDING]]).limit(10)
    #return collection.find({'created_at': {'$gte': datetime.datetime(2016,07,28,00,00,00), '$lte': datetime.datetime(2016,07,28,23,59,59)}}).sort([['created_at', pymongo.DESCENDING]]).limit(10)
	
	# datas passadas por parametros
	# return collection.find({'created_at': {'$gte': datetime.datetime(2016,07,28,00,00,00), '$lte': datetime.datetime(2016,07,28,23,59,59)}},no_cursor_timeout=False).sort([['created_at', pymongo.DESCENDING]])
	return collection.find({'created_at': {'$gte': datetime.datetime(2016,07,20,00,00,00), '$lte': datetime.datetime(2016,07,21,23,59,59)}},no_cursor_timeout=False)
	#return collection.find({'created_at': {'$gte': datetime.datetime(2016,07,28,00,00,00)}},no_cursor_timeout=False)

	#return collection.find()
    # not run

    #db.tweets_1000.find({'created_at': ISODate("2016-03-22T17:27:28Z")}).sort({'_id':-1}).limit(1)
    

    #return collection.find({'created_at': {$gte:ISODate("2016-06-27T00:00:00Z"), $lte:ISODate("2016-06-27T01:00:00Z")}})
    #, {'_id', pymongo.DESCENDING})
    # {$gte:d, $lte:d1}
    #db.coll.find({createdDate:{$and:[{$gte:ISODate("2015-01-23 10:00:00Z"), $lte:ISODate("2015-01-23 16:00:00Z")}, {$gte:ISODate("2015-01-24 10:00:00Z"), $lte:ISODate("2015-01-24 16:00:00Z")}, {$gte:ISODate("2015-01-25 10:00:00Z"), $lte:ISODate("2015-01-25 16:00:00Z")}]})

# db.tweets_1000.find({'created_at': {$lte: ISODate("2016-03-22T17:27:29Z"), $gte: ISODate("2016-03-22T17:27:28Z")}}).sort({'_id':-1})


'''PRINT SOME FIELS OF THE JSON'''
def operations_dates(records, paths, output_file):
    temp = 0
    # Save the results
    # Colocar a data do dump no arquivo
    # http://stackoverflow.com/questions/11280382/python-mongodb-pymongo-json-encoding-and-decoding
    arq = open("test2.json", "a+")
    for record in records:
        output_filter = dict_find(paths, record)
        arq.write(dumps(output_filter)+'\n')
        
        # Time UTC, then 00:00 > 03:00
        # output_filter['human_date'] =  record['created_at']
        # date = record['created_at']
        # print "DATE:", date
        temp+=1
        # arq.write(str(otp)+'\n')
        # arq.write("\n")
        # arq.write(dumps(record))
        
        #date = record['created_at']
        #print date
        # if(date < datetime.datetime(2016,07,28,23,59,59)):
        #     temp +=1
        #     print date
        # #else:
        #    print 'finish'
        # '''
	       #  # Text
	       #  text = (emoji_pattern.sub('', record['text'])).replace(";", ",").replace("\n", " ").encode('utf-8')
	       #  print "TEXT:", text

	       #  # Tweet ID
	       #  tweet_id = str(record['_id']).encode('utf-8')
	       #  print "TWEET ID:", tweet_id
	 
	       #  # Latitude and Longitude
	       #  try:
	       #      longitude = str(record['coordinates']['coordinates'][0]).encode('utf-8')
	       #      latitude = str(record['coordinates']['coordinates'][1]).encode('utf-8')
	       #  except TypeError:
	       #      longitude = ""
	       #      latitude = ""
	       #  print "LAT,LONG:", latitude, longitude

	       #  # Language
	       #  language = record['lang'].encode('utf-8')
	       #  print "LANGUAGE:", language

	       #  # Date in human readble format. The original value is in miliseconds since 1970.
	       #  date = record['created_at']
	       #  print "DATE:", date

	       #  # User screen name
	       #  user_name = record['user']['screen_name'].encode('utf-8')
	       #  print "USER NAME:", user_name

	       #  # User id
	       #  user_id = str(record['user']['id']).encode('utf-8')
	       #  print "USER ID:", user_id

	       #  users_id_mention = [user_mention['id'] for user_mention in record['entities']['user_mentions']]
	       #  print "USER MENTIONED: ", users_id_mention

	       #  if 'retweeted_status' in record:
	       #  	user_retweeted = record['retweeted_status']['user']['id']
	       #  	print "User retweeted:" , user_retweeted
	       #  	#print "User retweeted:", user_retweeted


	       #  # Collection
	       #  try:
	       #      collection = record['control']['coletas'][0]['id']
	       #  except:
	       #      collection = "Unknown"
	       #  print "COLLECTION:", collection

	       #  print "\n\n"
        # '''
    print 'Quantidade de registros:', temp


'''PRINT SOME FIELS OF THE JSON'''
def operations(records):
    temp = 0
    for record in records:
        date = record['created_at']
        print date
        # if(date < datetime.datetime(2016,07,28,23,59,59)):
        #     temp +=1
        #     print date
        # #else:
        #    print 'finish'
        # '''
	       #  # Text
	       #  text = (emoji_pattern.sub('', record['text'])).replace(";", ",").replace("\n", " ").encode('utf-8')
	       #  print "TEXT:", text

	       #  # Tweet ID
	       #  tweet_id = str(record['_id']).encode('utf-8')
	       #  print "TWEET ID:", tweet_id
	 
	       #  # Latitude and Longitude
	       #  try:
	       #      longitude = str(record['coordinates']['coordinates'][0]).encode('utf-8')
	       #      latitude = str(record['coordinates']['coordinates'][1]).encode('utf-8')
	       #  except TypeError:
	       #      longitude = ""
	       #      latitude = ""
	       #  print "LAT,LONG:", latitude, longitude

	       #  # Language
	       #  language = record['lang'].encode('utf-8')
	       #  print "LANGUAGE:", language

	       #  # Date in human readble format. The original value is in miliseconds since 1970.
	       #  date = record['created_at']
	       #  print "DATE:", date

	       #  # User screen name
	       #  user_name = record['user']['screen_name'].encode('utf-8')
	       #  print "USER NAME:", user_name

	       #  # User id
	       #  user_id = str(record['user']['id']).encode('utf-8')
	       #  print "USER ID:", user_id

	       #  users_id_mention = [user_mention['id'] for user_mention in record['entities']['user_mentions']]
	       #  print "USER MENTIONED: ", users_id_mention

	       #  if 'retweeted_status' in record:
	       #  	user_retweeted = record['retweeted_status']['user']['id']
	       #  	print "User retweeted:" , user_retweeted
	       #  	#print "User retweeted:", user_retweeted


	       #  # Collection
	       #  try:
	       #      collection = record['control']['coletas'][0]['id']
	       #  except:
	       #      collection = "Unknown"
	       #  print "COLLECTION:", collection

	       #  print "\n\n"
        # '''
    print 'Quantidade de registros:', temp


'''MAIN FUNCTION'''
if ( __name__ == "__main__" ):
    get_parameters()
    connect_db()
    database = client[args['database']]
    collection = database[args['collection']]
	
    startDate = args['startDate']
    endDate = args['endDate']

    output_file = args['output']

    # Working with date
    # https://pymotw.com/2/datetime/

    # Format the date
    fmt = '%Y-%m-%d %H:%M:%S'
    # fmt = '%Y-%m-%d'

    print startDate
    print endDate

    # daylight saving time dates for brazil
    # http://www.timeanddate.com/time/zone/brazil/brasilia
    # -03:06. Why?
    brazil_tz = pytz.timezone('America/Sao_Paulo')
    # -03:00
    gmt_tz = pytz.timezone('Etc/GMT+3')

    print pytz.utc
 

    x = datetime.datetime.strptime(endDate,fmt)
    x = x.replace(tzinfo=gmt_tz)
    # x = x.replace(tzinfo=pytz.utc)
    test = datetime.datetime.strftime(x,fmt)
    print test
    #x.replace(tzinfo=pytz.timezone("GMT"))

    y = datetime.datetime.strptime(startDate,fmt)
    y = y.replace(tzinfo=gmt_tz)
    # y = y.replace(tzinfo=pytz.utc)


    test2 = datetime.datetime.strftime(y,fmt)
    print test2

    print 'Data inicial:', y
    # print 'Data inicial:', y.isoformat()
    print 'Data final:', x
    # print 'Data inicial:', x.isoformat()
    # print datetime.datetime(2016,1,30,00,00,00)


    # x = datetime.datetime.strftime(endDate,fmt)
    # y = datetime.datetime.strftime(startDate,fmt)
    # print 'Data inicial:', y
    # print 'Data final:', x

    # print x.year
    # print x.month
    # print x.day
    # print x.microsecond
    # print x.tzinfo

    #print retrieve_data_d1(collection,y,x)
    
    # y = datetime.datetime.strptime(startDate,"%m/%d/%Y")
    # print y
    paths = [
	    "_id", "id", "text", "created_at", "geo", "place", "coordinates", "entities", 
	    "_tmp_", "control", "lang",
	
	    "user.id", "user.screen_name", "user.location", "user.profile_image_url", 
	    "user.profile_image_url_https", "user.friends_count", "user.followers_count", 
	    "user.description", "user.lang",
	
	    "retweeted_status.id", "retweeted_status.text", "retweeted_status.created_at",
	    "retweeted_status.user.id", "retweeted_status.user.screen_name",
	    "retweeted_status.retweet_count", "retweeted_status.entities"
	]

    paths_selected = [
        "_id", "id", "text", "created_at", "geo", "place", "coordinates", "entities", 
	    "_tmp_", "control", "lang", "source", "user.id", "user.screen_name", "user.location", "user.profile_image_url", 
	    "user.profile_image_url_https", "user.friends_count", "user.followers_count", "user.description", "user.lang",
	    "retweeted_status.id", "retweeted_status.text", "retweeted_status.created_at",
	    "retweeted_status.user.id", "retweeted_status.user.screen_name",
	    "retweeted_status.retweet_count", "retweeted_status.entities"
	    ]
    #print paths

    # y = y + datetime.timedelta(hours=-3)
    # x = x + datetime.timedelta(hours=-3)
    # print "Acrescimo de GMT-3 hrs", y
    # print "Acrescimo de GMT-3 hrs", x


    print "=== Query Starting"

    records = retrieve_data_from_dates(collection,y,x)

    print "=== Query Finished"



    output_file = output_file+"__"+re.sub(" ","_",startDate)+"_"+re.sub(" ","_",endDate)+".json"
    print output_file


    startime = time.time()
    operations_dates (records,paths_selected,output_file)
    stoptime = time.time()
    print "Time to retrievel data from collection: ", stoptime-startime
	# datetime.datetime.strptime("1/6/2016", "%m/%d/%Y")
	# datetime.datetime(2016, 1, 6, 0, 0)

 #    startime = time.time()
 #    records = retrieve_data(collection)
 #    stoptime = time.time()
 #    print "Time to retrievel data from collection: ", stoptime-startime

 #    startime = time.time()
 #    operations(records)
 #    stoptime = time.time()
 #    print "Time to perfom operations on retrieve data: ", stoptime-startime
    
	#    import dateutil.parser

	# my_date_str = "2011-01-01T16:00:00Z"

	# dateutil.parser.parse(my_date_str) 
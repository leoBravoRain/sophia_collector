# Script for:
# 1) Connect to Twitter API, get tweets and store in MongoDB
# 2) Take the tweets from MongoDB and:
# 	a) Get URL to website of new
# 	b) Download content from URL
# 	c) Clean the article and get the text of news
# 	d) Store the text in MongoDB


# Script

# Libraries for production
import time # for a pause from queries to API
import json
import sys # For catching errors
from pymongo import MongoClient
#import pymongo # Client for mongo

# Libraries for testing
import random # Just for testing
import requests


#Parametros en relación con la autentificación en Twitter
PARAM_TOKEN = "2177040169-5aVazrfgCpjhDOgemcIw1PvZXb3nbHtglUuAOcf";
PARAM_SECRET = "iCVFeVZkhSC3hsJfISbLDbpZZAyUS2Grn6ZJUNABaWmrt";
PARAM_CONSUMERKEY = "ac1yMlhXpxDAjzJWwmzagg";
PARAM_CONSUMERSECRET = "wpMMIXxkZ3ChqANkdVkzMH0wMdb8nKMqIVaztIEwtw";
PARAM_WAITING_TIME = 5; # secons (5 minutos for production)
PARAM_TWITTERAPI_URL_TIMELINE = "https://api.twitter.com/1.1/statuses/home_timeline.json";
PARAM_NBRESULT="?count=200";

# Paremetros en relación con el uso de Mongo para almacenar temporalmente los tweets 
database_name ="SophiaCollectorNew";
collection_name ="Tweets";

# Global parameters
# mongo_collection = 0
# Message to system
print('Creating Mongo client')
# Connect to mongo running in localhost
client = MongoClient('localhost', 27017)
# Get DB
db = client[database_name]
# Get collection
mongo_collection = db[collection_name]


# Get last tweets from API
# Return a JSON Array with tweets
# Addapt to Twitter API
def get_last_Tweets():

	print('Getting last tweets from API')

	# initializate list
	tweets = []

	# Try do request
	try:

		# Do query to API (It must to be to Twitter API)
		# Return a json collection
		# Fake data
		req = requests.get('https://jsonplaceholder.typicode.com/posts')

		# This depend of anwser of API
		tweets = json.loads(req.text)

	# Exception
	except Exception as e:

		# Print error
		print('Error trying get tweets from API. \n Error: \n {0}'.format(e))

	# Return list
	return tweets

# Get the id of the last tweet in DB
def get_last_tweet_id_in_Mongo():

	# Check if there is an object in DB
	if(mongo_collection.count() > 0):

		# Get last tweet id
		last_id = mongo_collection.find().sort('id', -1)[0]['id']

		print('last tweet: {0}'.format(last_id))

	# If there is not register in DB
	else:

		last_id = 0

	return last_id


# Function for get tweets
def Collect_Tweets_from_Twitter_to_Mongo():

	# Get collection Tweets from Mongo DB 
	# mongo_collection = create_connection_to_DB()

	# Do queries
	while True:

		# get last tweets from API
		# Return a list of tweets (each tweet is a dictionary)
		last_tweets = get_last_Tweets()

		# get last tweet id from mongo
		last_tweet_id_in_Mongo = get_last_tweet_id_in_Mongo()

		# Get lenght of tweets from api
		num_tweets = len(last_tweets)

		# # Iterate through each tweet
		# # Maybe do better algorithm: search for id smaller than last one, and delete all them. So the foor loop will run in less operation
		for tweet in last_tweets:

			# If tweet have not been stored in DB
			if(tweet['id'] > last_tweet_id_in_Mongo):

				# add flag 'to download'
				tweet['to_download'] = 1

				# insert tweet to mongo collection
				print('Add tweet {0} to MongoDB collection'.format(tweet['id']))
				mongo_collection.insert_one(tweet)


		# Wait a moment for next query
		try:

			# Send message to system
			print('Pause')

			# Sleep bucle for a moment 
			time.sleep(PARAM_WAITING_TIME)

		# Get exception
		except Exception as e:

			# Send error to system
			print('Error while pausing bucle.\nError: \n{0}'.format(e));

# # Create connection to Mongo DB
# def create_connection_to_DB():

# 	# Message to system
# 	print('Creating Mongo client')

# 	# Connect to mongo running in localhost
# 	client = MongoClient('localhost', 27017)

# 	# Get DB
# 	db = client[database_name]

# 	# Get collection
# 	collection = db[collection_name]

# 	return collection

Collect_Tweets_from_Twitter_to_Mongo()
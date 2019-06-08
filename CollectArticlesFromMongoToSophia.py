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
from newspaper import Article # scrapping article form
#import pymongo # Client for mongo

# Libraries for testing
import random # Just for testing
import requests


#Parametros en relación con la autentificación en Twitter
PARAM_WAITING_TIME = 5 # 1 minuto
PARAM_DOWNLOAD_AND_WAIT = 5; # 50 by default

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


# Check if tweet has URL
def tweet_has_URL(tweet):

	has_url = True

	return has_url

# Sleep function for wait until next download
def sleep():

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

# Download news from tweet
def download_article(tweet):

	# Get urls from tweet
	#urls = tweet.get("entities").get("urls")
	urls = [

		'https://www.latercera.com/pulso/noticia/banco-central-remece-al-mercado-anuncia-mayor-recorte-tasas-desde-la-crisis-financiera/690206/',
		'https://www.lun.com/Pages/NewsDetail.aspx?dt=2019-06-08&NewsID=429387&BodyID=0&PaginaId=26',
	]	

	# Itera
	# Download article using newspaper library
	article = Article(url)

	return article

# Function for get tweets
def Collect_Tweets_from_Mongo_to_Sophia_API():

	# Get collection Tweets from Mongo DB 
	# mongo_collection = create_connection_to_DB()

	# Do queries
	while True:

		# Read MongoDB to find new tweets
		# Not analized elements have flag value 1 in 'to_download'
		tweets = mongo_collection.find({ 'to_download': 1 }).limit(50)

		print(tweets[209]['id'])

		# print(tweets.next()['id'])

		# Get lenght of tweets from MongoDB
		# Maybe this method can be better without do same query to DB (Maybe some property of cursor prior object)
		num_tweets = mongo_collection.find({ 'to_download': 1 }).limit(50).count(with_limit_and_skip=True)
		# num_tweets = mongo_collection.count_documents({ 'to_download': 1 })


		# If there is tweets in DB
		if(num_tweets > 0):

			print('Num tweets > 0')

			# Count downloads
			counter_download = 0

			# Iter over each tweet
			for tweet in tweets:

				print(tweet['id'])

				# Check if twet has URL
				if(tweet_has_URL(tweet)):

					print('Tweet has URL')

					# Update donwloading counter
					counter_download += 1

					# If reach the max number of downloads
					if(counter_download == PARAM_DOWNLOAD_AND_WAIT):

						print('Download max reached')

						# Update download counter
						counter_download = 0

						# Sleep until next download
						sleep()

					try:

						#sleep random time for scrapping
						#RANDOM_WAITING_TIME = ThreadLocalRandom.current().nextInt(1000, 5000);
						#Thread.sleep(RANDOM_WAITING_TIME);
						sleep()

						#This tweet contains URL, download it
						article = download_article(tweet)

				else:

					print('Tweet hasn"t URL')

			# tweet = tweets[0]

			# it_tweets = tweets.iterator

		# Here it must to sleep 
		else: 

			# Here must to sleep
			print('Num tweets 0 from Mongo DB\nIt MUST to sleep')

		# # get last tweets from API
		# # Return a list of tweets (each tweet is a dictionary)
		# last_tweets = get_last_Tweets()

		# # get last tweet id from mongo
		# last_tweet_id_in_Mongo = get_last_tweet_id_in_Mongo()


		# # # Iterate through each tweet
		# # # Maybe do better algorithm: search for id smaller than last one, and delete all them. So the foor loop will run in less operation
		# for tweet in last_tweets:

		# 	# If tweet have not been stored in DB
		# 	if(tweet['id'] > last_tweet_id_in_Mongo):

		# 		# add flag 'to download'
		# 		tweet['to_download'] = 1

		# 		# insert tweet to mongo collection
		# 		print('Add tweet {0} to MongoDB collection'.format(tweet['id']))
		# 		mongo_collection.insert_one(tweet)


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

Collect_Tweets_from_Mongo_to_Sophia_API()
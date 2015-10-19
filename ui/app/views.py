from app import app
from flask import Flask, render_template, request, url_for,jsonify
from cassandra.cluster import Cluster
import json
import urllib
import urllib2
import oauth2
from kafka.client import KafkaClient
from kafka.producer import SimpleProducer

cluster = Cluster(['ec2-54-215-252-32.us-west-1.compute.amazonaws.com']) 
session = cluster.connect('prod') 

client = KafkaClient('ec2-54-193-76-148.us-west-1.compute.amazonaws.com')
producer = SimpleProducer(client)

API_HOST = 'api.yelp.com'
BUSINESS_PATH = '/v2/business/'

CONSUMER_KEY = None
CONSUMER_SECRET = None
TOKEN = None
TOKEN_SECRET = None

# Takes in business_id based on yelp format 'business-name-city'and returns metadata about the business
def get_business(business_id):
	path = BUSINESS_PATH + business_id
	url = 'http://{0}{1}?'.format(API_HOST, urllib.quote(path.encode('utf8')))

	# Get authorization using oauth2 protocol
	consumer = oauth2.Consumer(CONSUMER_KEY, CONSUMER_SECRET)
	oauth_request = oauth2.Request(method="GET", url=url)

	oauth_request.update(
		{
		    'oauth_nonce': oauth2.generate_nonce(),
		    'oauth_timestamp': oauth2.generate_timestamp(),
		    'oauth_token': TOKEN,
		    'oauth_consumer_key': CONSUMER_KEY
		}
	)
	token = oauth2.Token(TOKEN, TOKEN_SECRET)
	oauth_request.sign_request(oauth2.SignatureMethod_HMAC_SHA1(), consumer, token)
	signed_url = oauth_request.to_url()

	conn = urllib2.urlopen(signed_url, None)
	try:
	    response = json.loads(conn.read())
	finally:
	    conn.close()

	return response

@app.route('/')
def home():
	return render_template("index.html")

@app.route('/signup')
def signup():
        return render_template("signup.html",msg = "We are currently unable to accept new users please click on the usernames link to view recommendations for existing accounts")

@app.route('/login')
def login():
	global user_id 
	global recommendations 
	user_id = request.args.get('username')
	recommendations = []
	# If username is not given, prompt for signup
	if user_id != "" : 
		# Get the most recent restaurant liked by the user
		query = "SELECT * FROM prod.seeds WHERE user_hashid = %s" % user_id
		response = session.execute(query)

		# When user does not exist
		if len(response) < 0:
			return render_template("signup.html", msg = "User ID not found")
		
		# Get the list of top 20 similar restaurants along with ranks
		restaurant_hashid = response[0][1]
		query = "SELECT * FROM prod.ranks WHERE restaurant_hashid = %s ORDER BY rank LIMIT 20" % restaurant_hashid
		response = session.execute(query)
		
		# When no matching restaurant exists
		if len(response) < 0:
                        return render_template("signup.html", msg = "No matching restaurants found")

		# Add to list of recommendations
		for i in range(0, len(response)) :
			query = "SELECT name_city from prod.idmapper where restaurant_hashid = %s" % response[i][2]
			name_city = session.execute(query)
			
			if len(name_city) > 0 :
				# Concatenate tuple name_city to construct yelp format business id
				business_id = name_city[0][0][0]+"-"+name_city[0][0][1]
				
				# Remove special characters
				business_id = business_id.replace(" ", "-").replace("'", "").replace(":","").lower()
				recommendations.append(business_id)		
		
		# Return info about top restaurant
		if len(recommendations) > 0:
			# URL of the business to redirect to in case of yes
			url = 'http://www.yelp.com/biz/' + recommendations[0]
			
			# Display image of the latest restaurant review 
			# Constrained to 100x100 size
			img = get_business(recommendations[0]).get('image_url')
			
			return render_template("recommendation.html",restaurant_pic = img, url = url, counter=0)
	else:
		return render_template("signup.html", msg = "No user name was specified, please refer to Usernames tab")

@app.route('/next')
def get_next():
	global user_id
	global recommendations
	# Implies the user clicked no, so increment the counter to get the next item in the recommendation list
	count = int(request.args.get('counter'))
	count = count + 1

	# This implies user did not like the previous recommendation so send appropriate message to kafka with low rating. 
	# we're not concerned with actual value
	msg = "%s | %s | 1" % (user_id, recommendations[count - 1])
	producer.send_messages('user_input', msg)

	# Get redirect url and metadata
	img = get_business(recommendations[count]).get('image_url')
	url = 'http://www.yelp.com/biz/' + recommendations[count]
	return render_template("recommendation.html", restaurant_pic = img, url = url, counter = str(count))

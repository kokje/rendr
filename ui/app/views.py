from app import app
from flask import Flask, render_template, request, url_for,jsonify
from cassandra.cluster import Cluster
import json
import urllib
import urllib2
import oauth2

cluster = Cluster(['ec2-54-215-252-32.us-west-1.compute.amazonaws.com']) 
session = cluster.connect('temp') 

recommendations = []

API_HOST = 'api.yelp.com'
BUSINESS_PATH = '/v2/business/'

CONSUMER_KEY = None
CONSUMER_SECRET = None
TOKEN = None
TOKEN_SECRET = None

def get_business(business_id):
	path = BUSINESS_PATH + business_id
	url = 'http://{0}{1}?'.format(API_HOST, urllib.quote(path.encode('utf8')))

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

@app.route('/login')
def login():
	user_id = request.args.get('username')
	if user_id != "" : 
		#query = "SELECT * FROM temp.seeds WHERE user_id = '%s'" % user_id
		#response = session.execute(query)
		#restaurant_hashid = response[0][1]
		restaurant_hashid = 8393030393921923931
		query = "SELECT * from temp.ranks  WHERE restaurant_hashid = %s" % restaurant_hashid
		response = session.execute(query)
		size = len(response)-1
		for i in range(size, size-5, -1) :
			query = "SELECT name_city from temp.idmapper where restaurant_hashid = %s" % response[i][2]
			name_city = session.execute(query)
			business_id = name_city[0][0][0]+"-"+name_city[0][0][1]
			business_id = business_id.replace(' ', '-').lower()
			recommendations.append(business_id)		
	return render_template("recommendation.html", counter=1)

@app.route('/next')
def get_next():
	count = int(request.args.get('counter'))
	count = count + 1
	#get_business(recommendations[count])
	img = '/home/ubuntu/rendr/ui/app/stocimg.jpg'
	url = 'http://www.yelp.com/biz/' + recommendations[count]
	return render_template("recommendation.html", restaurant_pic = img, url = url, counter = str(count))

from app import app
from flask import Flask, render_template, request, url_for,jsonify
from cassandra.cluster import Cluster
import urllib2
import oauth2

CLUSTER = Cluster(['ec2-54-215-252-32.us-west-1.compute.amazonaws.com']) 
SESSION = CLUSTER.connect('temp') 

API_HOST = 'api.yelp.com'
BUSINESS_PATH = '/v2/business/'

CONSUMER_KEY = None
CONSUMER_SECRET = None
TOKEN = None
TOKEN_SECRET = None

def get_business(business_id):
    """Query the Business API by a business ID. Returns JSON response from the request"""
    business_path = BUSINESS_PATH + business_id
    return request(API_HOST, business_path)

@app.route('/')
def home():
	return render_template("index.html")

@app.route('/login')
def getResults():
	user_id = request.args.get('username')
	url = 'http://yelp.com'
	if user_id != "" : 
		query = "SELECT * FROM temp.seeds WHERE user_id = '%s'" % user_id
		response = session.execute(query)
		restaurant_hashid = response[0][1]
		query = "SELECT * from temp.ranks" # WHERE restaurant_hashid = %s" % restaurant_hashid
		response = session.execute(query)
		response_list = []
		size = len(response)-1
		for i in range(size, size-5, -1) :
			query = "SELECT * from temp.idmapper where restaurant_hashid = %s" % response[i][2]
			recommendation = session.execute(query)
			response_list.append(recommendation)
		jsonresponse = [{"restaurant" : x[0][1]} for x in response_list]	
	return render_template("recommendation.html", url = url)

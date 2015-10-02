from app import app
from flask import Flask, render_template, request, url_for,jsonify


@app.route('/')
def home():
	return render_template("index.html")

@app.route('/login')
def getResults():
	user_id = request.args.get("username")
	return "hello"
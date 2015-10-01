from app import app
from flask import Flask, render_template, request, url_for,jsonify


@app.route('/')
def home():
	return render_template("index.html")

@app.route('/user/<user_id>')
def getResults(user_id):
	return "Hello"
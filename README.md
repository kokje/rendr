# Rendr
Project built during the Insight Data Engineering program

*Work in progress, feedback is really appreciated*

## Index
1. [Introduction] (README.md#1-introduction)
2. [AWS Clusters] (README.md#2-aws-clusters)
3. [Data Pipeline] (README.md#3-data-pipeline)
4. [Front End] (README.md#4-front-end)

## 1. Introduction
Rendr is an application that builds a bipartite graph of users and restaurants to make recommendations using the structure of this network.
A user is shown a restaurant based on how popular it is with people who are similar to the user

### Data Sources
Foursquare Data collected by University of Minnesota researchers obtained from Internet Archives containing 2,153,471 users, 1,143,092 venues, 121,970 check-ins and 2,809,581 ratings that users assigned to venues; all extracted from the Foursquare application through the public API  
[Yelp Data](http://www.yelp.com/dataset_challenge) obtained from the Yelp Academic Dataset Challenge consisting of 1.6M reviews and 500K tips by 366K users for 61K businesses
## 2. AWS Clusters 
Rendr is powered by three clusters on AWS-
* 4 m4.larges for [Spark] (https://spark.apache.org/) and [HDFS] (http://hadoop.apache.org/docs/r1.2.1/hdfs_design.html)
* 3 t2.mediums for [Kafka] (http://kafka.apache.org/) and [Zookeeper] (https://zookeeper.apache.org/)
* 3 t2.mediums for [Cassandra](http://cassandra.apache.org/) and [Flask] (http://flask.pocoo.org/)

![Clusters] (/flask/static/img/clusters.png)

## 3. Data Pipeline

![Pipeline] (/flask/static/img/pipeline.png)

 * ### Data Collection and Ingestion 
  * The data collected from the sources is stored on [HDFS](http://hadoop.apache.org/docs/r1.2.1/hdfs_design.html) with 3 data nodes and 1 name node. 
  
  * 3 consumers are collecting data from the Rendr frontend and send messages to [Kafka](http://kafka.apache.org/) when the user performs an action such as liking the restaurant that was recommended. These messages are consumed using [camus] (https://github.com/linkedin/camus). [Camus] (https://github.com/linkedin/camus) is a tool built by [Linkedin] (https://www.linkedin.com/) which is essentially a distributed consumer running a map reduce job underneath to consume messages from [Kafka] (http://kafka.apache.org/) and save them to [HDFS](http://hadoop.apache.org/docs/r1.2.1/hdfs_design.html).
  
 * ### Batch Processing
  [Spark](http://spark.apache.org/docs/latest/index.html) and [Graphx](http://spark.apache.org/graphx) is used for all batch processing. 
  The data from Yelp and Foursquare has very diffferent schema. Foursquare data only contains latitude and longitude of the venue and no other metadata such as whether the venue is a restaurant or not, the name, city, state etc. This needs to be filtered against yelp data which is much richer. Geohashing is used for entity resolution to determine whether a rating in foursquare refers to a restaurant in yelp.
 
 * ### Serving Layer
  [Cassandra](http://cassandra.apache.org/) is used to to save the batch results and serve the front end. Three main tables serve the application-
  * Seeds- key is the username and value is the restaurant id of the most recent restaurant liked/reviewed by the user
  * Ranks - key is the restaurant id and values are the ranks and ids of other restaurants in the network
  * IdMapper - key is the restaurant id and value is the metadata of the restaurant such as name, city, state which is needed to construct the query to the yelp API

## 4. Front end
Used flask for the front end along with javascript, html and css for views


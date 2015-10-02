package rendr
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

case class Rating (user_id : Int, rating : Int)
case class Venue  (venue_id: Int, lat: Double, long: Double)

object FoursquareDataSet {
	
	def main(args : Array[String]) {
		val ratingsFile = "hdfs://ec2-54-193-103-208.us-west-1.compute.amazonaws.com:9000/foursquare/reviews_small.dat" // Dump of foursquare reviews
                val venuesFile = "hdfs://ec2-54-193-103-208.us-west-1.compute.amazonaws.com:9000/foursquare/venues_small.dat" // Dump of foursquare business metadata
		val businessesFile = "hdfs://ec2-54-193-103-208.us-west-1.compute.amazonaws.com:9000/foursquare/businesses_small.json" // Dump of yelp business metadata
                
		val sc = new SparkContext("local", "FoursquareDataSet", "/usr/local/spark",List("target/scala-2.10/foursquareproject_2.10-1.0.jar"))
		
		//val conf = new SparkConf().setAppName("FoursquareDataSet").set("spark.executor.memory", "2g").set("spark.eventLog.enabled", "true").set("spark.eventLog.dir", "/mnt/data/sparklog")
		//val sc = new SparkContext(conf)
		val sqlContext = new org.apache.spark.sql.SQLContext(sc)
		import sqlContext.implicits._
                import sqlContext._
		
		// Read foursquare ratings info from file
		// RDD[(Venue, Rating)]
		val ratings = sc.textFile(ratingsFile).map { line =>
      			val fields = line.split("\\s+")
       			(fields(3).toInt,Rating(fields(1).toInt, fields(5).toInt))
    		}.cache()
		//ratings.foreach(println)
		
		// Read fourquare venues info from file
		// RDD[Venue]
		val venues = sc.textFile(venuesFile).map { line =>
                        val fields = line.split("\\s+")
                        Venue(fields(1).toInt, fields(3).toDouble, fields(5).toDouble)
                }.cache()
		
		// Generate geohash of venues
		// RDD[Geohash, Venue]
		val hashedVenueFoursquare = venues.map(line => (GeoHash.encode(line.lat,line.long), line.venue_id))
		
		// Filter yelp businesses for restaurants
		// RDD[Business-> Restaurants]
		val businesses  = sqlContext.jsonFile(businessesFile).select("business_id","name","city","state","categories","latitude", "longitude").map(line  => Business.transform(line.getString(0), line.getString(1), line.getString(2), line.getString(3), line.getSeq(4), line.getDouble(5), line.getDouble(6))).filter(line => Business.filter(line))
		
		// Generate geohash of restaurants
		// RDD[Geohash, Business]
		val hashedVenueYelp = businesses.map(line => (GeoHash.encode(line.lat, line.long), line))
		
		// Join restaurants in yelp to venues in foursquare
		// RDD[Geohash, (Restaurant, Business)]
		val restaurantsYelpFoursquare = hashedVenueYelp.join(hashedVenueFoursquare).map(line => (line._2._2, line._2._1))
		
		// Join restaurants from foursquare with ratings
		// RDD[VenueID, (Restaurant,Rating)] -> RestaurantID | UserID | Rating
		restaurantsYelpFoursquare.join(ratings).map(line => (line._2._1.business_id, line._2._2.user_id, line._2._2.rating)).foreach(println)	
		// Filter ratings based on  		
	}
}

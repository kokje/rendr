package rendr
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

case class Rating (user_id : Int, venue_id : Int, rating : Int)
case class Venue  (venue_id : Int, lat : Double, long : Double)
case class Business  (business_id : String, lat : Double, long : Double)
case class HashedBusiness(business_id : String, geohash : String)
case class HashedVenue(venue_id : Int, geohash : String)

object FoursquareDataSet {
	
	def main(args : Array[String]) {
		val ratingsFile = "hdfs://ec2-54-193-103-208.us-west-1.compute.amazonaws.com:9000/foursquare/reviews_small.dat" // Dump of foursquare reviews
                val venuesFile = "hdfs://ec2-54-193-103-208.us-west-1.compute.amazonaws.com:9000/foursquare/venues_small.dat" // Dump of foursquare business metadata
		val businessesFile = "hdfs://ec2-54-193-103-208.us-west-1.compute.amazonaws.com:9000/yelp/businesses_small.json" // Dump of yelp business metadata
                
		val sc = new SparkContext("local", "FoursquareDataSet", "/usr/local/spark",List("target/scala-2.10/foursquareproject_2.10-1.0.jar"))
		
		//val conf = new SparkConf().setAppName("FoursquareDataSet").set("spark.executor.memory", "2g").set("spark.eventLog.enabled", "true").set("spark.eventLog.dir", "/mnt/data/sparklog")
		//val sc = new SparkContext(conf)
		val sqlContext = new org.apache.spark.sql.SQLContext(sc)
		import sqlContext.implicits._
                import sqlContext._

		val ratings = sc.textFile(ratingsFile).map { line =>
      			val fields = line.split("\\s+")
       			Rating(fields(1).toInt, fields(3).toInt, fields(5).toInt)
    		}.cache()
		//ratings.collect().foreach(println)
		
		val venues = sc.textFile(venuesFile).map { line =>
                        val fields = line.split("\\s+")
                        Venue(fields(1).toInt, fields(3).toDouble, fields(5).toDouble)
                }.cache()
		//venues.collect().foreach(println)
		val hashedVenueFoursquare = venues.map(line => HashedVenue(line.venue_id, GeoHash.encode(line.lat,line.long)))
		val businesses  = sqlContext.jsonFile(businessesFile).select("business_id", "latitude", "longitude").map(line => Business(line.getString(0), line.getDouble(1), line.getDouble(2)))
		val hashedVenueYelp = businesses.map(line => HashedBusiness(line.business_id, GeoHash.encode(line.lat, line.long)))
		hashedVenueYelp.collect().foreach(println)		
	}
}

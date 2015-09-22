/***SimpleApp.scala ***/
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

case class Restaurant (business_id: String, name: String, city: String, state: String )
case class UserGraph (business_id : String, user_id : String, name : String, city : String, state : String)

object SimpleApp {
        def getRestaurantsFromBusinesses(business_id : String, name: String, city: String, state: String, categories: Seq[String]) : Restaurant = {
                if (categories.contains("Restaurants")) {
                        Restaurant(business_id, name, city, state)
                }
                else {
                        Restaurant("-1", "-1", "-1","-1")
                }
        }

        def filterNulls(restaurant : Restaurant) : Boolean = {
                restaurant.business_id != "-1"
        }

        def main(args: Array[String]) {
                val reviewFile = "hdfs://ec2-54-183-27-164.us-west-1.compute.amazonaws.com:9000/yelp/reviews.json" // Dump of yelp reviews
                val businessFile = "hdfs://ec2-54-183-27-164.us-west-1.compute.amazonaws.com:9000/yelp/businesses.json" // Dump of yelp business metadata

                val conf = new SparkConf().setAppName("Simple App").setMaster("local").setJars(List("target/scala-2.10/simple-project_2.10-1.0.jar")).set("spark.cassandra.connection.host", "172.31.19.62")
                val sc = new SparkContext(conf)

                val sqlContext = new org.apache.spark.sql.SQLContext(sc)

                import sqlContext.implicits._
                import sqlContext._

                // Todo : Use schema before loading table
                val businessDF = sqlContext.jsonFile(businessFile).select("business_id","name","city","state","categories")

                // Todo : Review signs > 3 positive or negative
                val reviewsDF = sqlContext.jsonFile(reviewFile).select("business_id", "user_id")

                val restaurantRDD = businessDF.map(p => getRestaurantsFromBusinesses(p.getString(0), p.getString(1), p.getString(2), p.getString(3), p.getSeq(4))).filter(value => filterNulls(value))
                val joinedResult = reviewsDF.join(restaurantRDD.toDF(), "business_id")

                // Should we group by at this stage or dump everything?
                val collection = joinedResult.map(line => UserGraph(line.getString(0), line.getString(1), line.getString(2),line.getString(3), line.getString(4)))
                // Does this override or append?
                collection.saveToCassandra("test", "cities", SomeColumns("business_id", "user_id", "name","city", "state"))
        }
}
     
/***SimpleApp.scala ***/
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

case class Restaurant (business_id: String, name: String, city: String, state: String )
case class UserGraph (business_id : String, users : List[String])

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
                val reviewFile = "../reviews.json" // Dump of yelp reviews
                val businessFile = "../businesses.json" // Dump of yelp business metadata

                val sc = new SparkContext("local", "Simple App", "/usr/local/spark",
                List("target/scala-2.10/simple-project_2.10-1.0.jar"))
                val sqlContext = new org.apache.spark.sql.SQLContext(sc)

                import sqlContext.implicits._
                import sqlContext._

                // Todo : Use schema before loading table
                val businessDF = sqlContext.jsonFile(businessFile).select("business_id","name","city","state","categories")

                // Todo : Assign signs to reviews > 3 positive or negative
                val reviewsDF = sqlContext.jsonFile(reviewFile).select("business_id", "user_id")

                // Todo : Use other metadata or restaurants? Type of cuisine etc? For cooler analytics?
                val restaurantRDD = businessDF.map(p => getRestaurantsFromBusinesses(p.getString(0), p.getString(1), p.getString(2), p.getString(3), p.getSeq(4))).filter(value => filterNulls(value))
                val joinedResult = reviewsDF.join(restaurantRDD.toDF(), "business_id")

                // Todo: Use group by or not for graphx?
                joinedResult.show()
        }
}
                                                                                                                                                                                      1,1           All

/*** SimpleApp.scala ***/
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object SimpleApp {
        def filterRestaurants(categories : Seq[String]) : Boolean = {
                categories.contains("Restaurants")
        }

        def main(args: Array[String]) {
                val reviewFile = "../reviews.json" // Dump of yelp reviews
                val businessFile = "../businesses.json" // Dump of yelp business metadata

                val sc = new SparkContext("local", "Simple App", "/usr/local/spark",
                List("target/scala-2.10/simple-project_2.10-1.0.jar"))
                val sqlContext = new org.apache.spark.sql.SQLContext(sc)

                // Todo : Use schema before loading table
                val businessDF  = sqlContext.jsonFile(businessFile)

                // Dataframes are similar to rdbms tables so the filter function works on columns
                // Converted to RDD to allow custom row filtering.
                // Todo : Alternatively, try using foreach and create to generate a new DF. This is
                // necessary because objects are immutable and you can't simply delete rows
                val businessRDD = businessDF.select("business_id","categories","name","city","state").rdd
                val businessResult = businessRDD.filter(line => filterRestaurants(line.getSeq(1)))
                businessResult.collect().foreach(println)

                val reviewsDF = sqlContext.jsonFile(reviewFile)
                val reviewsRDD = reviewsDF.select("business_id", "user_id").rdd
                val reviewsResult = reviewsRDD.map(line => (line.get(0), line.get(1))).groupByKey()
                reviewsResult.collect().foreach(println)
        }
}
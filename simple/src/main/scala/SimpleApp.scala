/***SimpleApp.scala ***/
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._

case class Restaurant (business_id: String, name: String, city: String, state: String )
case class UserGraph (business_id : String, user_id : String, name : String, city : String, state : String)

case class VertexProperty(val vertex_id: Long, val name: String)

object SimpleApp {
	// Todo : Don't forget to make this dynamic
        def getRestaurantsFromBusinesses(business_id : String, name: String, city: String, state: String, categories: Seq[String]) : Restaurant = {
                if (categories.contains("Restaurants") && city=="Carnegie") {
                        Restaurant(business_id, name, city, state)
                }
                else {
                        Restaurant("-1", "-1", "-1","-1")
                }
        }

        def filterNulls(restaurant : Restaurant) : Boolean = {
                restaurant.business_id != "-1"
        }

        def computeAndSend(baseId : Long, rankedGraph : Graph[Double, Double] ) = {
                val rankedPairs = rankedGraph.vertices.filter(value => value._2 > 0.15).map(value => (baseId, value._2, value._1.asInstanceOf[Long]))
                rankedPairs.saveToCassandra("test", "ranks", SomeColumns("base_id","rank", "pair_id"))
        	//rankedPairs.collect().foreach(println)
	}

        def main(args: Array[String]) {
                val reviewFile = "hdfs://ec2-54-193-103-208.us-west-1.compute.amazonaws.com:9000/yelp/reviews_full.json" // Dump of yelp reviews
                val businessFile = "hdfs://ec2-54-193-103-208.us-west-1.compute.amazonaws.com:9000/yelp/businesses_full.json" // Dump of yelp business metadata

		val conf = new SparkConf().setAppName("Simple App").set("spark.cassandra.connection.host", "172.31.19.62")
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

		// Todo: What happens with and without distinct?		
		val restaurantPropertyRDD : RDD[(VertexId, String)] = joinedResult.select("business_id", "name").map(resto => (resto.getString(0).hashCode().asInstanceOf[VertexId], resto.getString(1))).distinct()
		val userPropertyRDD : RDD[(VertexId, String)] = joinedResult.select("user_id").map(user => (user.getString(0).hashCode().asInstanceOf[VertexId], "user")).distinct()
		
		val vertexRDD : RDD[(VertexId, String)] = userPropertyRDD.union(restaurantPropertyRDD)
		val edgeRDD : RDD[Edge[Int]] = joinedResult.map(line => Edge(line.getString(1).hashCode(), line.getString(0).hashCode(), 1))	
		
		// What do edges imply?
		val graph : Graph[String,Int] = Graph(vertexRDD, edgeRDD)
                restaurantRDD.collect().foreach(value => computeAndSend(value.business_id.hashCode(), graph.personalizedPageRank(value.business_id.hashCode(), 0.0001)))
	}
}

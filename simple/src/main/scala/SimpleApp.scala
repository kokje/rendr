/***SimpleApp.scala ***/
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

case class Restaurant (business_id: String, name: String, city: String, state: String )
case class UserGraph (business_id : String, user_id : String, name : String, city : String, state : String)

case class VertexProperty(val vertex_id: Long, val name: String)

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

	def computeAndSend(rankedGraph : Graph[Double, Double] ) = {
		val result = rankedGraph.vertices.filter(value => value._2 > 0.15).collect().foreach(println)
	}

        def main(args: Array[String]) {
                val reviewFile = "hdfs://ec2-54-183-27-164.us-west-1.compute.amazonaws.com:9000/yelp/reviews_small.json" // Dump of yelp reviews
                val businessFile = "hdfs://ec2-54-183-27-164.us-west-1.compute.amazonaws.com:9000/yelp/businesses_small.json" // Dump of yelp business metadata

		val conf = new SparkConf().setAppName("Simple App")
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
		
		val restaurantPropertyRDD : RDD[(VertexId, String)] = joinedResult.select("business_id", "name").map(resto => (resto.getString(0).hashCode().asInstanceOf[VertexId], resto.getString(1)))
		val userPropertyRDD : RDD[(VertexId, String)] = joinedResult.select("user_id").map(user => (user.getString(0).hashCode().asInstanceOf[VertexId], "user"))
		
		val vertexRDD : RDD[(VertexId, String)] = userPropertyRDD.union(restaurantPropertyRDD)
		val edgeRDD : RDD[Edge[Int]] = joinedResult.map(line => Edge(line.getString(1).hashCode(), line.getString(0).hashCode(), 1))	
		
		val graph : Graph[String,Int] = Graph(vertexRDD, edgeRDD)
		val result = graph.personalizedPageRank(restaurantPropertyRDD.first()._1, 0.0001)
		computeAndSend(result)
	}
}

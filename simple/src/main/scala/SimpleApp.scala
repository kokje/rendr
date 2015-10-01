/***SimpleApp.scala ***/
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import org.apache.spark.storage.StorageLevel

case class Restaurant (business_id: String, name: String, city: String, state: String )

object SimpleApp {
	// Todo : Don't forget to make this dynamic
        def getRestaurantsFromBusinesses(business_id : String, name: String, city: String, state: String, categories: Seq[String]) : Restaurant = {
		if (categories.contains("Restaurants")) {
                        Restaurant(business_id, name, city, state)
                }
                else {
                        Restaurant("-1", "-1", "-1","-1")
                }
        }

        def filterRestaurants(restaurant : Restaurant, state : String) : Boolean = {
                restaurant.business_id != "-1" && restaurant.state == state
        }

        def computeAndSend(baseId : Long, rankedGraph : Graph[Double, Double] ) = {
                val rankedPairs = rankedGraph.vertices.filter(value => value._2 > 0.15).map(value => (baseId, value._2, value._1.asInstanceOf[Long]))
               	rankedPairs.saveToCassandra("test", "ranks", SomeColumns("base_id","rank", "pair_id"))
        	//rankedPairs.collect().foreach(println)
	}

        def main(args: Array[String]) {
               	val reviewFile = "hdfs://ec2-54-193-103-208.us-west-1.compute.amazonaws.com:9000/yelp/r1.json" // Dump of yelp reviews
                val businessFile = "hdfs://ec2-54-193-103-208.us-west-1.compute.amazonaws.com:9000/yelp/b1.json" // Dump of yelp business metadata

		val conf = new SparkConf().setAppName("Simple App").set("spark.cassandra.connection.host", "172.31.19.62").set("spark.executor.memory", "2g").set("spark.eventLog.enabled", "true").set("spark.eventLog.dir", "/mnt/data/sparklog")
		val sc = new SparkContext(conf)

                val sqlContext = new org.apache.spark.sql.SQLContext(sc)

                import sqlContext.implicits._
                import sqlContext._

                // Todo : Use schema before loading table
                val businessDF = sqlContext.jsonFile(businessFile).select("business_id","name","city","state","categories")

                // Todo : Review signs > 3 positive or negative
                val reviewsDF = sqlContext.jsonFile(reviewFile).select("business_id", "user_id")
		
		// Todo: This is a static list so maybe read cities from a file?
		val states = List("PA")
		for (state <- states) {
 
                	val restaurantRDD = businessDF.map(p => getRestaurantsFromBusinesses(p.getString(0), p.getString(1), p.getString(2), p.getString(3), p.getSeq(4))).filter(value => filterRestaurants(value, state))
			// Todo: How do I optimize this join? Cache reviews table? 
			val joinedResult = reviewsDF.join(restaurantRDD.toDF(), "business_id")
			// Todo: What happens with and without distinct?		
			val restaurantPropertyRDD : RDD[(VertexId, String)] = joinedResult.select("business_id", "name").map(resto => (resto.getString(0).hashCode().asInstanceOf[VertexId], resto.getString(0))).distinct().persist(StorageLevel.MEMORY_AND_DISK_SER)
			val userPropertyRDD : RDD[(VertexId, String)] = joinedResult.select("user_id").map(user => (user.getString(0).hashCode().asInstanceOf[VertexId], "user")).distinct()
			
			joinedResult.select("user_id", "business_id").map(line => (line.getString(0), line.getString(1), line.getString(1).hashCode())).saveToCassandra("test", "seeds",SomeColumns("user_id","restaurant_id", "restaurant_id_num"))
			restaurantPropertyRDD.saveToCassandra("test", "idmapper",SomeColumns("restaurant_id_num", "restaurant_id"))
			
			val vertexRDD : RDD[(VertexId, String)] = userPropertyRDD.union(restaurantPropertyRDD)
			val edgeRDD : RDD[Edge[Int]] = joinedResult.map(line => Edge(line.getString(1).hashCode(), line.getString(0).hashCode(), 1))
		
			// What do edges imply?
			val graph : Graph[String,Int] = Graph(vertexRDD, edgeRDD)
			restaurantRDD.collect().foreach(value => computeAndSend(value.business_id.hashCode(), graph.personalizedPageRank(value.business_id.hashCode(), 0.0001)))
		}
	}
}

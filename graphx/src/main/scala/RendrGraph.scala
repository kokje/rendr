import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import com.datastax.spark.connector._
import com.google.common.hash.Hashing
import com.google.common.base.Charsets

case class CombinedEntry(restaurant_id : String, user_id : String, name : String, city : String, state : String) 
object RendrGraph {
	
	// Takes in a graph of vertices and edges and writes to cassandra
        def computeAndSend(baseId : Long, rankedGraph : Graph[Double, Double] ) = {
                
		// filter values below initial weight because they are user vertices
                val rankedPairs = rankedGraph.vertices.filter(value => value._2 > 0.15).map(value => (baseId, value._2, value._1.asInstanceOf[Long]))
                
		rankedPairs.saveToCassandra("prod", "ranks", SomeColumns("restaurant_hashid","rank", "match_hashid"))
                //rankedPairs.collect().foreach(println)
        }

	def hashId(id : String) : Long = {
                
		Hashing.md5().hashString(id, Charsets.UTF_8).asLong()
	}

	def main(args: Array[String]) {
                
		val foursquareFile = "hdfs://ec2-54-193-103-208.us-west-1.compute.amazonaws.com:9000/clean/foursquare_all.txt"
                val yelpFile = "hdfs://ec2-54-193-103-208.us-west-1.compute.amazonaws.com:9000/clean/yelp_all.txt" 

                val conf = new SparkConf().setAppName("RendrGraph").set("spark.cassandra.connection.host", "172.31.19.62").set("spark.executor.memory", "2g")//.set("spark.eventLog.enabled", "true").set("spark.eventLog.dir", "/mnt/data/sparklog")
                //val conf = new SparkConf().setAppName("RendrGraph").set("spark.executor.memory", "2g")
                val sc = new SparkContext(conf)

                val foursquareRDD = sc.textFile(foursquareFile).map(_.split(",")).map(line => CombinedEntry(line(0).drop(1), line(1), line(2), line(3), line(4).dropRight(1)))

                val yelpRDD = sc.textFile(yelpFile).map(_.split(",")).map(line => CombinedEntry(line(0).drop(1), line(1), line(2), line(3), line(4).dropRight(1)))
		// Todo: Repartition things here?
		val combinedRDD = foursquareRDD.union(yelpRDD)

		// Yelp data contains 10 major cities, one per state so use states to partition because info for each major city includes smaller neighboring towns as well
		// US, Canada, Germany, UK 
               	val states = List("PA","IL","NC","NV","AZ","WI","QC","ON","BW","EDH")
                for (state <- states) {

                        val shardRDD = combinedRDD.filter(line => line.state == state)
			
			// Using VertexRDD ensures that nodes are distinct and also uses a common index for RDDs                
			val restaurantVertexRDD = VertexRDD(shardRDD.map{ line => (hashId(line.restaurant_id).asInstanceOf[VertexId],(line.name,line.city))}) 
                        restaurantVertexRDD.saveToCassandra("prod", "idmapper",SomeColumns("restaurant_hashid", "name_city"))
                        
			val userVertexRDD = VertexRDD(shardRDD.map{ line=> (hashId(line.user_id).asInstanceOf[VertexId],("user", "user"))})
                        shardRDD.map{ line => (hashId(line.user_id),hashId(line.restaurant_id))}.saveToCassandra("prod", "seeds",SomeColumns("user_hashid", "restaurant_hashid"))
			
			val vertexRDD = userVertexRDD.union(restaurantVertexRDD)
                        val edgeRDD = shardRDD.map(line => Edge(hashId(line.user_id), hashId(line.restaurant_id), 1))
                        
                        val graph  = Graph(vertexRDD, edgeRDD)
                        //computeAndSend(restaurantVertexRDD.first()._1, graph.personalizedPageRank(restaurantVertexRDD.first()._1, 0.0001))
                        restaurantVertexRDD.collect().foreach(line => computeAndSend(line._1, graph.personalizedPageRank(line._1, 0.0001)))
                }
        }
}

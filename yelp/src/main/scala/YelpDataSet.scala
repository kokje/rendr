/***Yelp.scala ***/
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.SparkContext._
import rendr.Business

object YelpProject {
       
	 def main(args: Array[String]) {

               	val reviewFile = "hdfs://ec2-54-193-103-208.us-west-1.compute.amazonaws.com:9000/yelp/reviews_full.json" // Dump of yelp reviews
                val businessFile = "hdfs://ec2-54-193-103-208.us-west-1.compute.amazonaws.com:9000/yelp/businesses_full.json" // Dump of yelp business metadata
		
		//val conf = new SparkConf().setAppName("YelpProject").set("spark.executor.memory", "2g").set("spark.eventLog.enabled", "true").set("spark.eventLog.dir", "/mnt/data/sparklog")
		val conf = new SparkConf().setAppName("YelpProject").set("spark.executor.memory", "2g")
		val sc = new SparkContext(conf)

                val sqlContext = new org.apache.spark.sql.SQLContext(sc)

                import sqlContext.implicits._
                import sqlContext._

                val businessDF = sqlContext.jsonFile(businessFile).select("business_id","name","city","state","categories")
                // Todo : Review signs > 3 positive or negative
                val reviewsDF = sqlContext.jsonFile(reviewFile).select("business_id", "user_id")

		val restaurantRDD = businessDF.map(p => Business.transform(p.getString(0), p.getString(1), p.getString(2), p.getString(3), p.getSeq(4))).filter(line=> line.business_id != -1)
		
		val joinedResult = reviewsDF.join(restaurantRDD.toDF(), "business_id")
		joinedResult.collect().foreach(println)
	}
}

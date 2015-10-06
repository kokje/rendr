sbt clean
sbt package
spark-submit --class YelpProject --master local target/scala-2.10/yelpproject_2.10-1.0.jar

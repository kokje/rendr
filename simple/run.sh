sbt clean
sbt assembly
sbt package
spark-submit --class SimpleApp --master spark://ip-172-31-4-211:7077 --jars target/scala-2.10/simpleproject_2.10-1.0.jar, target/scala-2.10/SimpleProject-assembly-1.0.jar

name := "SimpleProject"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-core" % "1.4.1",
"org.apache.spark" %% "spark-sql" % "1.4.1",
"org.apache.spark" %% "spark-graphx" % "1.4.1",
"com.datastax.spark" %% "spark-cassandra-connector" % "1.4.0"
)

mergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}

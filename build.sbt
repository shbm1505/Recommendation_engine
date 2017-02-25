name := "Simple Project"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.0"

libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.6.0"

libraryDependencies += "datastax" % "spark-cassandra-connector" % "1.6.0-s_2.10"

resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"

libraryDependencies ++= Seq(
  "org.apache.cassandra" % "cassandra-thrift" % "3.5" ,
  "org.apache.cassandra" % "cassandra-clientutil" % "3.5",
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.0.0",
  "mysql" % "mysql-connector-java" % "5.1.39"
)

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.5.2"

libraryDependencies += "org.apache.commons" % "commons-io" % "1.3.2"

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.5.2"

libraryDependencies ++= Seq(
  "org.eclipse.jetty.orbit" % "javax.servlet" % "3.0.0.v201112011016" artifacts (
    Artifact("javax.servlet", "jar", "jar")
  )
)

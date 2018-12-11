name := "cassandraSpark"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion

//The version of the spark-cassandra-connector has to be bumped only up when [[sparkVersion]] is bumped up
// https://mvnrepository.com/artifact/com.datastax.spark/spark-cassandra-connector
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-avro
libraryDependencies += "org.apache.spark" %% "spark-avro" % "2.4.0"

libraryDependencies += "world.data" % "dw-jdbc" % "0.4.1"

import java.util.Properties

import org.apache.spark.sql.SparkSession

object DataWorld extends App {
  val appName = "cassandraConnect"
  val master = "local[*]"
  val spark = SparkSession.builder
    .appName(appName)
    .master(master)
    .getOrCreate()

  val username = "kusumakarb"
  val token = "Replace this with the actual token!!!"

  val df = spark.read
    .format("jdbc")
    .option("url", "jdbc:data:world:sql:kusumakarb:numbers")
    .option("driver", "world.data.jdbc.Driver")
    .option("username", username)
    .option("password", token)
    .option("query", "Select col1, col2 from testTwoDates")
    .load()

  df.show()

  val connectionProperties = new Properties()

  connectionProperties.put("user", username)
  connectionProperties.put("password", token)

  df.write
    .mode("Overwrite")
    .jdbc("jdbc:data:world:sql:kusumakarb:allstars",
          "allstar",
          connectionProperties)
}

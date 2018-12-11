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
  val token =
    "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJwcm9kLXVzZXItY2xpZW50Omt1c3VtYWthcmIiLCJpc3MiOiJhZ2VudDprdXN1bWFrYXJiOjo1NDNlYTc3My0xNTI3LTQ1OTgtYjJiYi0wZTQ4YTRmOTlhMmMiLCJpYXQiOjE1NDI5MTAyMTQsInJvbGUiOlsidXNlcl9hcGlfcmVhZCIsInVzZXJfYXBpX3dyaXRlIl0sImdlbmVyYWwtcHVycG9zZSI6dHJ1ZX0._NJHnzmIQgMg-vtwsidCf1tU7HhphJs9jcnns9ZHmmMmBaTJAFmQAO7DaG2RcXzhEy4qrYHt6NoumjGqLVFGlQ"

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

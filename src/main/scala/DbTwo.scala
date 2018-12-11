import org.apache.spark.sql.SparkSession

object DbTwo extends App {
//  username: 'db2user1',
//  password: 'w8wfy99DvEmgkBsE',
//  host: 'db2.test.plotly.host',
//  port: 50000,
//  database: 'plotly'

  val appName = "cassandraConnect"
  val master = "local[*]"
  val spark = SparkSession.builder
    .appName(appName)
    .master(master)
    .getOrCreate()

  val df = spark.read
    .format("jdbc")
    .option("url", "jdbc:db2://db2.test.plotly.host:50000/SYSIBM")
    .option("driver", "com.ibm.db2.jcc.DB2Driver")
    .option("user", "db2user1")
    .option("password", "w8wfy99DvEmgkBsE")
    .option("query", "SELECT * FROM SYSIBM.SYSTABLES WHERE type = 'T'")
//    .option("dbtable", "plotly.ALCOHOL_CONSUMPTION_BY_COUNTRY_2010")
    .load()

  df.show()

}

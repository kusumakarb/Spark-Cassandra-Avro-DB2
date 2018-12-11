import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object Cassandra extends App {
  val appName = "cassandraConnect"
  val master = "local[*]"
  val spark = SparkSession.builder
    .appName(appName)
    .master(master)
    .getOrCreate()

  val keyspace = "tutorialspoint"
  val table = "emp"

  private val sc: SparkContext = spark.sparkContext
  implicit val cassndraConf = CassandraConnector(
    sc.getConf
      .set("spark.cassandra.connection.host", "localhost")
      .set("spark.cassandra.auth.username", "cassandra")
      .set("spark.cassandra.auth.password", "cassandra")
      .set("spark.cassandra.connection.ssl.clientAuth.enabled", "true")
      .set("spark.cassandra.connection.ssl.enabled", "true")
      .set("spark.cassandra.connection.ssl.keyStore.password", "cassandra")
      .set("spark.cassandra.connection.ssl.keyStore.path",
           "/opt/cassandra/conf/certs/cassandra.keystore")
      .set("spark.cassandra.connection.ssl.trustStore.password", "cassandra")
      .set("spark.cassandra.connection.ssl.trustStore.path",
           "/opt/cassandra/conf/certs/cassandra.truststore")
  )

  sc.cassandraTable(keyspace, table).foreach(println)
  //emp_id: 3, emp_city: Chennai, emp_name: rahman, emp_phone: 9848022330, emp_sal: 45000
  val columns = Seq("emp_id", "emp_city", "emp_name", "emp_phone", "emp_sal")
  val collection =
    sc.parallelize(Seq((100, "Visakhapatnam", "Cosmo", 9848022330L, 45000)))
  collection.saveToCassandra(
    keyspace,
    table,
    SomeColumns("emp_id", "emp_city", "emp_name", "emp_phone", "emp_sal"))

  val df = spark.createDataFrame(collection)

//  df.write.cassandraFormat()
//  sc.cassandraTable(keyspace, table).foreach(println)

  //  val conf = new SparkConf()
  //    .setAppName(appName)
  //    .setMaster(master)
  //    .set("spark.cassandra.connection.host", "localhost")

  //  val sc = new SparkContext(conf)
  //  sc.cassandraTable(keyspace, table).foreach(println)
//  val connector: CassandraConnector = CassandraConnector()
//  val newSession = spark.newSession()

}

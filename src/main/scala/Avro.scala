import org.apache.spark.sql.{SaveMode, SparkSession}

object Avro extends App {
  val spark = SparkSession.builder().master("local").getOrCreate()

  // The Avro records get converted to Spark types, filtered, and
  // then written back out as Avro records
  val df = spark.read
    .format("avro")
    .load("/home/kusumakar/Downloads/avros/userdata1.avro")
  df.show()
//  private val filteredDf: Dataset[Row] = df.filter("doctor > 5")
//  filteredDf.show()
  df.write
    .format("avro")
    .option("compression", "bzip2")
    .mode(SaveMode.Overwrite)
    .save("/tmp/bzip")
}

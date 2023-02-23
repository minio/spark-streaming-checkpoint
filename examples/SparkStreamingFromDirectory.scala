import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object SparkStreamingFromDirectory {

  def main(args: Array[String]): Unit = {

    val spark:SparkSession = SparkSession.builder()
      .appName("SparkByExample")
      .config("spark.sql.streaming.checkpointFileManagerClass", "io.minio.spark.checkpoint.S3BasedCheckpointFileManager")
      .master("local[1]").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "http://127.0.0.1:9000")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "minioadmin")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "minioadmin")

    val schema = StructType(
      List(
        StructField("RecordNumber", IntegerType, true),
        StructField("Zipcode", StringType, true),
        StructField("ZipCodeType", StringType, true),
        StructField("City", StringType, true),
        StructField("State", StringType, true),
        StructField("LocationType", StringType, true),
        StructField("Lat", StringType, true),
        StructField("Long", StringType, true),
        StructField("Xaxis", StringType, true),
        StructField("Yaxis", StringType, true),
        StructField("Zaxis", StringType, true),
        StructField("WorldRegion", StringType, true),
        StructField("Country", StringType, true),
        StructField("LocationText", StringType, true),
        StructField("Location", StringType, true),
        StructField("Decommisioned", StringType, true)
      )
    )

    val df = spark.readStream
      .schema(schema)
      .json("./resources/")

    df.printSchema()

    val groupDF = df.select("Zipcode")
        .groupBy("Zipcode").count()
    groupDF.printSchema()

    groupDF.writeStream
      .format("console")
      .outputMode("complete")
      .option("truncate", false)
      .option("newRows", 30)      
      .option("checkpointLocation", "s3a://process-runner/checkpoints/")
      .start()
      .awaitTermination()
  }
}

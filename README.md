# Spark Streaming Checkpoint File Manager for MinIO

This project implements a new [CheckpointFileManager](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/CheckpointFileManager.scala) a different checkpoint logic for MinIO. In this approach, instead of relying on PUT, COPY, DELETE approach of doing renames(), the task writes the checkpoint data directly to the final object it intends to.

Once writing data to the object is complete, the associated output stream is closed. MinIO guarantees that a object is visible only when the output stream is closed. This way the rename operation is avoided, therefore ensuring there is no file listing call while writing data and no consistency issue. Also, it is more efficient as copying the entire content from one object to another object is also avoided.

## Sample Code used in testing

```scala
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
```

The resources used for streaming inputs.
```
tree ../resources/
../resources/
├── zipcode10.json
├── zipcode11.json
├── zipcode12.json
├── zipcode1.json
├── zipcode2.json
├── zipcode3.json
├── zipcode4.json
├── zipcode5.json
├── zipcode6.json
├── zipcode7.json
├── zipcode8.json
└── zipcode9.json

0 directories, 12 files
```

## Results (concise)

### Optimization can be seen in terms of total time taken for Batch '0'
| Without Optimization | With Optimization |
|----------------------|-------------------|
| 72secs               | 17secs            |

### Total number of namespace pollution
| Total DEL markers without optimization | Total DEL markers with optimization |
|----------------------------------------|-------------------------------------|
| 409                                    | 0                                   |

### Total number of excess objects on namespace
| Total excess objects without optimization | Total excess objects with optimization |
|-------------------------------------------|----------------------------------------|
| 818 (out of which 409 are DEL markers)    | 0                                      |

### Total number of API calls
| Total number of API calls without optimization | Total number of API calls with optimization |
|------------------------------------------------|---------------------------------------------|
| 6938                                           | 224                                         |

### The number of excess calls to object ratio 
| API Calls / Objects without optimization | API Calls / objects with optimization |
|------------------------------------------|---------------------------------------|
| 33.8x                                    | 1.09x                                 |

*These results show the overall benefits of using this CheckpointFileManager, and why the upstream s3a based checkpointing is poorly designed to be used with object storage.*

## Results (detailed) with each steps

### Spark-shell with S3A based checkpointing

```
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.3.2
      /_/
         
scala> :load SparkStreamingFromDirectory-S3A.scala
Loading SparkStreamingFromDirectory-S3A.scala...
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
defined object SparkStreamingFromDirectory

scala> SparkStreamingFromDirectory.main(Array(""))
23/02/25 02:14:14 WARN SparkSession: Using an existing Spark session; only runtime SQL configurations will take effect.
root
 |-- RecordNumber: integer (nullable = true)
 |-- Zipcode: string (nullable = true)
 |-- ZipCodeType: string (nullable = true)
 |-- City: string (nullable = true)
 |-- State: string (nullable = true)
 |-- LocationType: string (nullable = true)
 |-- Lat: string (nullable = true)
 |-- Long: string (nullable = true)
 |-- Xaxis: string (nullable = true)
 |-- Yaxis: string (nullable = true)
 |-- Zaxis: string (nullable = true)
 |-- WorldRegion: string (nullable = true)
 |-- Country: string (nullable = true)
 |-- LocationText: string (nullable = true)
 |-- Location: string (nullable = true)
 |-- Decommisioned: string (nullable = true)

root
 |-- Zipcode: string (nullable = true)
 |-- count: long (nullable = false)

-------------------------------------------                                     
Batch: 0
-------------------------------------------
+-------+-----+
|Zipcode|count|
+-------+-----+
|76166  |2    |
|32564  |2    |
|85210  |2    |
|36275  |3    |
|709    |3    |
|35146  |3    |
|708    |2    |
|35585  |3    |
|32046  |2    |
|27203  |4    |
|34445  |2    |
|27007  |4    |
|704    |10   |
|27204  |4    |
|34487  |2    |
|85209  |2    |
|76177  |4    |
+-------+-----+
```

Amount of calls
```
mc support top api myminio/

API                             RX      TX      CALLS   ERRORS 
s3.CopyObject                   48 KiB  47 KiB  208     0     
s3.DeleteMultipleObjects        146 KiB 47 KiB  417     0     
s3.DeleteObject                 32 KiB  0 B     211     0     
s3.GetObject                    168 B   1.3 KiB 1       0     
s3.HeadObject                   441 KiB 0 B     2950    0     
s3.ListObjectsV2                408 KiB 1.4 MiB 2732    0     
s3.PutObject                    128 KiB 0 B     419     0     

Summary:

Total: 6938 CALLS, 1.2 MiB RX, 1.5 MiB TX - in 72.36s
```

The amount of files left over in the wake of this behavior on a versioned buckets.

```
~ mc ls -r --versions myminio/process-runner/ | wc -l
1023
```

Our of which `614` actual objects

```
~  mc ls -r --versions myminio/process-runner/ | grep PUT | wc -l
614
```

and almost `409` delete markers (soft deletes)

```
~ mc ls -r --versions myminio/process-runner/ | grep DEL | wc -l
409
```

Actual objects on namespace without versioning lookup
```
~ mc ls -r myminio/process-runner/  | wc -l
205
```

### After Direct Checkpointing Write Optimization

```
...
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.3.2
      /_/
         
Using Scala version 2.12.15 (OpenJDK 64-Bit Server VM, Java 11.0.17)
Type in expressions to have them evaluated.
Type :help for more information.

scala> :load SparkStreamingFromDirectory.scala
Loading SparkStreamingFromDirectory.scala...
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
defined object SparkStreamingFromDirectory

scala> SparkStreamingFromDirectory.main(Array(""))
23/02/25 02:20:25 WARN SparkSession: Using an existing Spark session; only runtime SQL configurations will take effect.
root
 |-- RecordNumber: integer (nullable = true)
 |-- Zipcode: string (nullable = true)
 |-- ZipCodeType: string (nullable = true)
 |-- City: string (nullable = true)
 |-- State: string (nullable = true)
 |-- LocationType: string (nullable = true)
 |-- Lat: string (nullable = true)
 |-- Long: string (nullable = true)
 |-- Xaxis: string (nullable = true)
 |-- Yaxis: string (nullable = true)
 |-- Zaxis: string (nullable = true)
 |-- WorldRegion: string (nullable = true)
 |-- Country: string (nullable = true)
 |-- LocationText: string (nullable = true)
 |-- Location: string (nullable = true)
 |-- Decommisioned: string (nullable = true)

root
 |-- Zipcode: string (nullable = true)
 |-- count: long (nullable = false)

-------------------------------------------                                     
Batch: 0
-------------------------------------------
+-------+-----+
|Zipcode|count|
+-------+-----+
|76166  |2    |
|32564  |2    |
|85210  |2    |
|36275  |3    |
|709    |3    |
|35146  |3    |
|708    |2    |
|35585  |3    |
|32046  |2    |
|27203  |4    |
|34445  |2    |
|27007  |4    |
|704    |10   |
|27204  |4    |
|34487  |2    |
|85209  |2    |
|76177  |4    |
+-------+-----+
```

```
~ mc support top api myminio/

API                     RX      TX      CALLS   ERRORS 
s3.GetObject            159 B   1.3 KiB 1       0     
s3.HeadObject           1.5 KiB 0 B     10      0     
s3.ListObjectVersions   765 B   2.0 KiB 5       0     
s3.PutObject            88 KiB  0 B     208     0     

Summary:

Total: 224 CALLS, 90 KiB RX, 3.3 KiB TX - in 17.00s
```

Actual number of valid objects 
```
~ mc ls -r --versions myminio/process-runner/ | wc -l
205
```

Actual objects on namespace without versioning lookup
```
~ mc ls -r myminio/process-runner/  | wc -l
205
```

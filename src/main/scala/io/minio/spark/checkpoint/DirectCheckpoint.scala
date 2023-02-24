package io.minio.spark.checkpoint

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

import java.nio.file.Paths
import java.nio.file.Files
import java.net.URI
import java.io.{FileNotFoundException, OutputStream}

import org.apache.spark.sql.execution.streaming.CheckpointFileManager
import org.apache.spark.sql.errors.QueryExecutionErrors

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.local.{LocalFs, RawLocalFs}
import org.apache.hadoop.fs.permission.FsPermission

import org.apache.spark.sql.execution.streaming.CheckpointFileManager._

// For AmazonS3 client
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3URI
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.VersionListing
import com.amazonaws.services.s3.model.ListVersionsRequest
import com.amazonaws.services.s3.model.S3VersionSummary
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration

/** An implementation of [[CheckpointFileManager]] using Native S3 APIs. */
class S3BasedCheckpointFileManager(path: Path, hadoopConfiguration: Configuration)
  extends CheckpointFileManager {

  private val API_PATH_STYLE_ACCESS = s"fs.s3a.path.style.access"
  private val SERVER_ENDPOINT = s"fs.s3a.endpoint"
  private val SERVER_REGION = s"fs.s3a.region"

  private val pathStyleAccess = hadoopConfiguration.get(API_PATH_STYLE_ACCESS, "true") == "true"
  private val endpoint = hadoopConfiguration.get(SERVER_ENDPOINT, "http://127.0.0.1:9000")
  private val location = hadoopConfiguration.get(SERVER_REGION, "us-east-1")
  println(s"#constructor(${endpoint})")
  private val s3Client =
    AmazonS3ClientBuilder.standard()
      .withCredentials(Credentials.load(hadoopConfiguration))
      .withPathStyleAccessEnabled(pathStyleAccess)
      .withEndpointConfiguration(new EndpointConfiguration(endpoint, location))
      .build()

  override def list(path: Path, filter: PathFilter): Array[FileStatus] = {
    println(s"#list(${path})")
    var p = path.toString().stripPrefix("s3a://").trim

    // Remove leading SEPARATOR
    if (!p.isEmpty()) {
      if (p.charAt(0) == Path.SEPARATOR_CHAR) {
        p = p.substring(1)
      }
    }

    val objectPos = p.indexOf(Path.SEPARATOR_CHAR)
    val bucketName = p.substring(0, objectPos);
    val prefix = p.substring(objectPos + 1);
    println(s"#list(${prefix})")
    println(s"#list(${bucketName})")

    var listVersionsResponse = s3Client.listVersions(bucketName, prefix)
    var results = ArrayBuffer[FileStatus]()
    listVersionsResponse.getVersionSummaries().foreach(s3Version => {
      results += newFile(s3Version)
    })

    while (listVersionsResponse.isTruncated()) {
      listVersionsResponse = s3Client.listNextBatchOfVersions(listVersionsResponse)
      listVersionsResponse.getVersionSummaries().foreach(s3Version => {
        results += newFile(s3Version)
      })
    }

    results.toArray
  }

  def newFile(version: S3VersionSummary): FileStatus = {
    new FileStatus(version.getSize(), false, 1, 64 * 1024 * 1024,
      version.getLastModified().getTime(), new Path(version.getBucketName(), version.getKey()))
  }

  override def mkdirs(path: Path): Unit = {
    // mkdirs() is bogus call, not needed on object
    // storage avoid it.
    println(s"#mkdirs(${path})")
  }

  override def createAtomic(path: Path, overwriteIfPossible: Boolean): CancellableFSDataOutputStream = {
    println(s"#createAtomic(${path}, ${overwriteIfPossible})")

    var p = path.toString().stripPrefix("s3a://").trim

    // Remove leading SEPARATOR
    if (!p.isEmpty()) {
      if (p.charAt(0) == Path.SEPARATOR_CHAR) {
        p = p.substring(1)
      }
    }

    val objectPos = p.indexOf(Path.SEPARATOR_CHAR)
    val bucketName = p.substring(0, objectPos);
    val objectName = p.substring(objectPos + 1);
    if (objectName.isEmpty()) {
      throw new IllegalArgumentException(path + " is not a valid path for the file system")
    }

    val outputStream = new S3OutputStream(s3Client, bucketName, objectName)

    new CancellableFSDataOutputStream(outputStream) {
      override def cancel(): Unit = {
        outputStream.cancel()
      }

      override def close(): Unit = {
        outputStream.close()
      }
    }
  }

  override def open(path: Path): FSDataInputStream = {
    println(s"#open(${path})")

    var p = path.toString().stripPrefix("s3a://").trim

    // Remove leading SEPARATOR
    if (!p.isEmpty()) {
      if (p.charAt(0) == Path.SEPARATOR_CHAR) {
        p = p.substring(1)
      }
    }

    val objectPos = p.indexOf(Path.SEPARATOR_CHAR)
    val bucketName = p.substring(0, objectPos)
    val objectName = p.substring(objectPos + 1)
    if (objectName.isEmpty()) {
      throw new IllegalArgumentException(path + " is not a valid path for the file system")
    }

    new FSDataInputStream(new S3InputStream(s3Client, bucketName, objectName))
  }

  override def exists(path: Path): Boolean = {
    println(s"#exists(${path})")

    var p = path.toString().stripPrefix("s3a://").trim

    // Remove leading SEPARATOR
    if (!p.isEmpty()) {
      if (p.charAt(0) == Path.SEPARATOR_CHAR) {
        p = p.substring(1)
      }
    }

    val objectPos = p.indexOf(Path.SEPARATOR_CHAR)
    val bucketName = p.substring(0, objectPos)
    val objectName = p.substring(objectPos + 1)
    if (objectName.isEmpty()) {
      throw new IllegalArgumentException(path + " is not a valid path for the file system")
    }

    s3Client.doesObjectExist(bucketName, objectName)
  }

  override def delete(path: Path): Unit = {
    println(s"#delete(${path})")
    deleteObjectsInBucket(path)
  }

  def deleteObjectsInBucket(path: Path): Unit = {
    var p = path.toString().stripPrefix("s3a://").trim

    // Remove leading SEPARATOR
    if (!p.isEmpty()) {
      if (p.charAt(0) == Path.SEPARATOR_CHAR) {
        p = p.substring(1)
      }
    }

    val objectPos = p.indexOf(Path.SEPARATOR_CHAR)
    val bucketName = p.substring(0, objectPos)
    val prefix = p.substring(objectPos + 1)

    var listVersionsResponse = s3Client.listVersions(bucketName, prefix)
    listVersionsResponse.getVersionSummaries().foreach(s3Version => {
      s3Client.deleteVersion(
        s3Version.getBucketName(),
        s3Version.getKey(),
        s3Version.getVersionId(),
      )
    })

    while (listVersionsResponse.isTruncated()) {
      listVersionsResponse = s3Client.listNextBatchOfVersions(listVersionsResponse)
      listVersionsResponse.getVersionSummaries().foreach(s3Version => {
        s3Client.deleteVersion(
          s3Version.getBucketName(),
          s3Version.getKey(),
          s3Version.getVersionId(),
        )
      })
    }
  }

  override def isLocal: Boolean = false

  override def createCheckpointDirectory(): Path = {
    println(s"#mkdirs(${path})")
    // No need to create the checkpoints folder
    // this is also another bogus requirement
    path
  }
}

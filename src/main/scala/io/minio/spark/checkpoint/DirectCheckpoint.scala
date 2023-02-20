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

  private val pathStyleAccess = hadoopConfiguration.get(API_PATH_STYLE_ACCESS, "false") == "true"
  private val endpoint = hadoopConfiguration.get(SERVER_ENDPOINT, "https://s3.amazonaws.com")
  private val location = hadoopConfiguration.get(SERVER_REGION, "us-east-1")
  private val s3Client =
    AmazonS3ClientBuilder.standard()
      .withCredentials(Credentials.load(hadoopConfiguration))
      .withPathStyleAccessEnabled(pathStyleAccess)
      .withEndpointConfiguration(new EndpointConfiguration(endpoint, location))
      .build()

  override def list(path: Path, filter: PathFilter): Array[FileStatus] = {
    val uri = path.toUri()
    var p = uri.getPath()

    // Remove leading SEPARATOR
    if (!p.isEmpty()) {
      if (p.charAt(0) == Path.SEPARATOR_CHAR) {
	p = p.substring(1)
      }
    }

    val objectPos = p.indexOf(Path.SEPARATOR_CHAR)
    val bucketName = p.substring(0, objectPos);
    val prefix = p.substring(objectPos + 1);

    var listVersionsResponse = new VersionListing
    var results = ArrayBuffer[FileStatus]()

    while (listVersionsResponse == null || listVersionsResponse.isTruncated()) {
      if (listVersionsResponse == null) {
        listVersionsResponse = s3Client.listVersions(bucketName, prefix)
      } else {
        listVersionsResponse = s3Client.listNextBatchOfVersions(listVersionsResponse)
      }
      listVersionsResponse.getVersionSummaries().foreach(s3Version => {
        results += newFile(s3Version)
      })
    }

    if (results.isEmpty) {
      throw new FileNotFoundException("Cannot find " + path.toUri())
    }
    
    results.toArray
  }

  def newFile(version: S3VersionSummary): FileStatus = {
    new FileStatus(version.getSize(), false, 1, 64 * 1024 * 1024,
      version.getLastModified().getTime(), new Path(version.getBucketName(), version.getKey()))
  }

  override def mkdirs(path: Path): Unit = {
    val uri = path.toUri()
    var p = uri.getPath()

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
    
    s3Client.putObject(bucketName, objectName, "")
  }

  override def createAtomic(path: Path, overwriteIfPossible: Boolean): CancellableFSDataOutputStream = {
    val uri = path.toUri()
    var p = uri.getPath()

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
    val uri = path.toUri()
    var p = uri.getPath()

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
    
    new FSDataInputStream(s3Client.getObject(bucketName, objectName).getObjectContent())
  }

  override def exists(path: Path): Boolean = {
    val uri = path.toUri()
    var p = uri.getPath()

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
    deleteObjectsInBucket(path)
  }

  def deleteObjectsInBucket(path: Path): Unit = {
    val uri = path.toUri()
    var p = uri.getPath()

    // Remove leading SEPARATOR
    if (!p.isEmpty()) {
      if (p.charAt(0) == Path.SEPARATOR_CHAR) {
	p = p.substring(1)
      }
    }

    val objectPos = p.indexOf(Path.SEPARATOR_CHAR)
    val bucketName = p.substring(0, objectPos)
    val prefix = p.substring(objectPos + 1)

    var listVersionsResponse = new VersionListing

    while (listVersionsResponse == null || listVersionsResponse.isTruncated()) {
      if (listVersionsResponse == null) {
        listVersionsResponse = s3Client.listVersions(bucketName, prefix)
      } else {
        listVersionsResponse = s3Client.listNextBatchOfVersions(listVersionsResponse)
      }
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
    this.mkdirs(path)
    path
  }  
}

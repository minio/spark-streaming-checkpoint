/*
 * Checkpoint File Manager for MinIO (C) 2023 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.minio.spark.checkpoint

// For AmazonS3 client
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3URI
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ObjectListing
import com.amazonaws.services.s3.model.S3ObjectSummary
import com.amazonaws.services.s3.model.AmazonS3Exception
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import org.apache.spark.sql.execution.streaming.CheckpointFileManager
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.local.{LocalFs, RawLocalFs}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.spark.sql.execution.streaming.CheckpointFileManager._
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

import java.nio.file.Paths
import java.nio.file.Files
import java.net.URI
import java.io.{FileNotFoundException, OutputStream}

/** An implementation of [[CheckpointFileManager]] using Native S3 APIs. */
class S3BasedCheckpointFileManager(path: Path, hadoopConfiguration: Configuration)
  extends CheckpointFileManager {

  private val API_PATH_STYLE_ACCESS = s"fs.s3a.path.style.access"
  private val SERVER_ENDPOINT = s"fs.s3a.endpoint"
  private val SERVER_REGION = s"fs.s3a.region"

  private val pathStyleAccess = hadoopConfiguration.get(API_PATH_STYLE_ACCESS, "true") == "true"
  private val endpoint = hadoopConfiguration.get(SERVER_ENDPOINT, "http://127.0.0.1:9000")
  private val location = hadoopConfiguration.get(SERVER_REGION, "us-east-1")
  private val s3Client =
    AmazonS3ClientBuilder.standard()
      .withCredentials(Credentials.load(hadoopConfiguration))
      .withPathStyleAccessEnabled(pathStyleAccess)
      .withEndpointConfiguration(new EndpointConfiguration(endpoint, location))
      .build()

  override def list(path: Path, filter: PathFilter): Array[FileStatus] = {
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

    var objectsResponse = s3Client.listObjects(bucketName, prefix)
    var results = ArrayBuffer[FileStatus]()
    objectsResponse.getObjectSummaries().foreach(s3Object => {
      results += newFile(s3Object)
    })

    while (objectsResponse.isTruncated()) {
      objectsResponse = s3Client.listNextBatchOfObjects(objectsResponse)
      objectsResponse.getObjectSummaries().foreach(s3Object => {
        results += newFile(s3Object)
      })
    }

    results.toArray
  }

  def newFile(obj: S3ObjectSummary): FileStatus = {
    new FileStatus(obj.getSize(), false, 1, 64 * 1024 * 1024,
      obj.getLastModified().getTime(), new Path(obj.getBucketName(), obj.getKey()))
  }

  override def mkdirs(path: Path): Unit = {
    // mkdirs() is bogus call, not needed on object storage avoid it.
    // this is a no-op.
  }

  override def createAtomic(path: Path, overwriteIfPossible: Boolean): CancellableFSDataOutputStream = {
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

  def isEmpty(x: String) = x == null || x.trim.isEmpty

  override def delete(path: Path): Unit = {
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

    try {
      val objectMeta = s3Client.getObjectMetadata(bucketName, objectName)
      objectMeta.getVersionId() match {
        case vid if isEmpty(vid) => {
          // Always delete the latest for unversioned.
          s3Client.deleteVersion(
            bucketName,
            objectName,
            "null",
          )
        }
        case vid => {
          // Always delete the latest.
          s3Client.deleteVersion(
            bucketName,
            objectName,
            objectMeta.getVersionId(),
          )
        }
      }
    } catch {
      case x: AmazonS3Exception =>
        if (x.getStatusCode() != 404) {
          throw x
        }
    }
  }

  override def isLocal: Boolean = false

  override def createCheckpointDirectory(): Path = {
    // No need to create the checkpoints folder
    // this is a bogus call, subsequent commit/delta
    // files automatically create this top level folder.
    path
  }
}

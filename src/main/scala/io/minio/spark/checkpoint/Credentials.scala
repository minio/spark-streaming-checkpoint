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

// For BasicAWSCredentials
import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.auth.BasicSessionCredentials
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import org.apache.hadoop.conf.Configuration

object Credentials {
  private def staticCredentialsProvider(credentials: AWSCredentials): AWSCredentialsProvider = {
    new AWSCredentialsProvider {
      override def getCredentials: AWSCredentials = credentials
      override def refresh(): Unit = {}
    }
  }

  def load(hadoopConfiguration: Configuration): AWSCredentialsProvider = {
    val accessKey = hadoopConfiguration.get(s"fs.s3a.access.key", null)
    val secretKey = hadoopConfiguration.get(s"fs.s3a.secret.key", null)
    val sessionToken = hadoopConfiguration.get(s"fs.s3a.session.token", null)
    if (accessKey != null && secretKey != null) {
      if (sessionToken != null) {
        Some(staticCredentialsProvider(new BasicSessionCredentials(accessKey, secretKey, sessionToken)))
      } else {
        Some(staticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
      }
    } else {
      None
    }
  }.getOrElse {
    // Finally, fall back on the instance profile provider
    new DefaultAWSCredentialsProviderChain()
  }
}

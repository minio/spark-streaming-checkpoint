package io.minio.spark.checkpoint

import java.net.URI

// For BasicAWSCredentials
import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.auth.BasicSessionCredentials
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain

import org.apache.hadoop.conf.Configuration

private[spark] object Credentials {
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

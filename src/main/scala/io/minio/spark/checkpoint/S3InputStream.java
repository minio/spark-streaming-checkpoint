/*
 * Direct Write Checkpointing (C) 2023 MinIO, Inc.
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

package io.minio.spark.checkpoint;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.hadoop.fs.FSInputStream;

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;

public class S3InputStream extends FSInputStream {
  /** The bucket-name on Amazon S3 */
  private final String bucket;

  /** The path (key) name within the bucket */
  private final String key;

  /** Amazon S3 client. * */
  private final AmazonS3 client;

  /** The current position of the seekable() InputStream */
  private long pos;

  /** indicates whether the stream is closed / valid */
  private boolean closed;

  /** the current getObject() object instance * */
  private S3Object wrappedObject;

  /** the current getObject() content stream * */
  private S3ObjectInputStream wrappedStream;

  /** the object metadata for the key * */
  private ObjectMetadata meta;

  /**
   * Creates a new S3 InputStream
   *
   * @param client the AmazonS3 client
   * @param bucket name of the bucket
   * @param key path within the bucket
   */
  public S3InputStream(AmazonS3 client, String bucket, String key) {
    this.client = client;
    this.bucket = bucket;
    this.key = key;

    this.pos = 0;
    this.closed = false;
    this.wrappedObject = null;
    this.wrappedStream = null;
    this.meta = client.getObjectMetadata(bucket, key);
  }

  private void openIfNeeded() throws IOException {
    if (wrappedObject == null) {
      reopen(0);
    }
  }

  private synchronized void reopen(long pos) throws IOException {
    if (wrappedStream != null) {
      wrappedStream.abort();
    }

    if (pos < 0) {
      throw new EOFException("Trying to seek to a negative offset " + pos);
    }

    if (meta.getContentLength() > 0 && pos > meta.getContentLength() - 1) {
      throw new EOFException("Trying to seek to an offset " + pos + " past the end of the file");
    }

    GetObjectRequest request = new GetObjectRequest(bucket, key);
    request.setRange(pos, meta.getContentLength() - 1);

    wrappedObject = client.getObject(request);
    wrappedStream = wrappedObject.getObjectContent();

    if (wrappedStream == null) {
      throw new IOException("Null IO stream");
    }

    this.pos = pos;
  }

  @Override
  public synchronized long getPos() throws IOException {
    return pos;
  }

  @Override
  public synchronized void seek(long pos) throws IOException {
    if (this.pos == pos) {
      return;
    }

    reopen(pos);
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }

  @Override
  public synchronized int read() throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }

    openIfNeeded();

    int byteRead;
    try {
      byteRead = wrappedStream.read();
    } catch (SocketTimeoutException e) {
      reopen(pos);
      byteRead = wrappedStream.read();
    } catch (SocketException e) {
      reopen(pos);
      byteRead = wrappedStream.read();
    }

    if (byteRead >= 0) {
      pos++;
    }

    return byteRead;
  }

  /**
   * Reads an array to the S3 Input Stream
   *
   * @param buf the byte array to read into
   * @param off the offset into the array
   * @param len the number of bytes to read
   */
  @Override
  public synchronized int read(byte[] buf, int off, int len) throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }

    openIfNeeded();

    int byteRead;
    try {
      byteRead = wrappedStream.read(buf, off, len);
    } catch (SocketTimeoutException e) {
      reopen(pos);
      byteRead = wrappedStream.read(buf, off, len);
    } catch (SocketException e) {
      reopen(pos);
      byteRead = wrappedStream.read(buf, off, len);
    }

    if (byteRead > 0) {
      pos += byteRead;
    }

    return byteRead;
  }

  @Override
  public synchronized void close() throws IOException {
    super.close();
    closed = true;
    if (wrappedObject != null) {
      wrappedObject.close();
    }
  }

  @Override
  public synchronized int available() throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }
    long remaining = this.meta.getContentLength() - this.pos;
    if (remaining > Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    }
    return (int) remaining;
  }

  @Override
  public boolean markSupported() {
    return false;
  }
}

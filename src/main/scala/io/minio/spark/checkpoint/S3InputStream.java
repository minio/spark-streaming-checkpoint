package io.minio.spark.checkpoint;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.hadoop.fs.FSInputStream;

import org.slf4j.Logger;

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.net.SocketException;

public class S3InputStream extends FSInputStream {
    private long pos;
    private boolean closed;
    private S3ObjectInputStream wrappedStream;
    private S3Object wrappedObject;
    private AmazonS3 client;
    private String bucket;
    private String key;
    private ObjectMetadata meta;

    public S3InputStream(AmazonS3 client, String bucket, String key) {
	this.bucket = bucket;
	this.key = key;
	this.client = client;
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

	if (meta.getContentLength() > 0 && pos > meta.getContentLength()-1) {
	    throw new EOFException("Trying to seek to an offset " + pos +
				   " past the end of the file");
	}

	GetObjectRequest request = new GetObjectRequest(bucket, key);
	request.setRange(pos, meta.getContentLength()-1);

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

    @Override
    public synchronized int read(byte buf[], int off, int len) throws IOException {
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
	return (int)remaining;
    }

    @Override
    public boolean markSupported() {
	return false;
    }
}

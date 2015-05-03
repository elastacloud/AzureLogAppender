package com.elastacloud.azure.blob.storage;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudPageBlob;
import com.microsoft.azure.storage.blob.CopyStatus;
import com.microsoft.azure.storage.blob.PageRange;

/**
 * Created by david on 10/04/14.
 */
public class PageBlobAppender {

	private CloudBlobContainer blobcontainer = null;
	private String logFileName = "";
	private int maxSize = 0;
	private final int PAGE_SIZE_MULTIPLE = 512;

	private int fileSuffix = 0;
	private List<PageRange> pages = null;

	private CloudStorageAccount storageAccount = null;

	private CloudPageBlob blob = null;
	private CloudBlobClient blobClient = null;

	private String message = null;

	private long writeOffset = 0;

	/*
	 * Constructor
	 */
	public PageBlobAppender() {

	}

	public PageBlobAppender(CloudStorageAccount storageAccount) {
		this.storageAccount = storageAccount;
	}

	private void appendToBlob() throws IOException, StorageException {
		// get a stream
		byte[] buffer = getAlignedBuffer(this.message.getBytes());
		InputStream inStream = new ByteArrayInputStream(buffer);

		// write the data
		this.blob.uploadPages(inStream, this.writeOffset, buffer.length);
	}

	/*
	 * Determines whether log file rollover is required based on the current last written point and the length of hte message in bytes
	 *
	 * @param fileSize - last written location in the file
	 *
	 * @param currentMessage - the current message to write
	 */
	private void checkRollover() throws URISyntaxException, StorageException {

		if (this.pages != null && this.pages.size() > 0 && this.pages.get(this.pages.size() - 1).getEndOffset() + this.message.getBytes().length > this.maxSize) {
			// rename
			rename();

			// recreate log file
			initBlob();
		}
	}

	private void deriveWriteOffset() throws StorageException {
		// check to see if we can append to the current page, if a page exists
		if (this.pages.size() > 0) {
			// get the most recent page
			PageRange lastPage = this.pages.get(this.pages.size() - 1);

			// get the last page and download the last 512 bytes
			byte[] buf = new byte[this.PAGE_SIZE_MULTIPLE];

			this.blob.downloadRangeToByteArray(lastPage.getEndOffset() - this.PAGE_SIZE_MULTIPLE + 1, (long) this.PAGE_SIZE_MULTIPLE, buf, 0);

			// find the first null reference in the file
			String str = new String(buf);
			int index = str.indexOf((byte) 0);

			// we have found a null
			if (index > 0) {
				// strip off any null characters
				String subStr = str.substring(0, str.indexOf((byte) 0));

				// append the message
				this.message = subStr + this.message;

				// set the write offset to be the start of the 512 bytes we downloaded
				this.writeOffset = lastPage.getEndOffset() - this.PAGE_SIZE_MULTIPLE + 1;
			} else {
				// otherwise write at the end of the last 512 bytes
				this.writeOffset = lastPage.getEndOffset() + 1;
			}
		}
	}

	/*
	 * Returns a new instance of the source aligned to 512 bytes
	 *
	 * @param source - the byte array source
	 */
	private byte[] getAlignedBuffer(byte[] source) {
		int remainder = source.length % this.PAGE_SIZE_MULTIPLE;
		int size = source.length;
		if (remainder > 0) {
			size += this.PAGE_SIZE_MULTIPLE - remainder;
		}

		byte[] buffer = Arrays.copyOf(ArrayUtils.EMPTY_BYTE_ARRAY, size);
		Arrays.fill(buffer, (byte) 0);
		System.arraycopy(source, 0, buffer, 0, source.length);

		return buffer;
	}

	public void getBlobReference(String connectionString, String containerName) throws Exception {
		this.storageAccount = CloudStorageAccount.parse(connectionString);
		this.blobClient = this.storageAccount.createCloudBlobClient();
		this.blobcontainer = this.blobClient.getContainerReference(containerName);
	}

	private void initBlob() throws URISyntaxException, StorageException {
		// get a reference to the blob
		this.blob = this.blobcontainer.getPageBlobReference(this.logFileName);

		// create if not exists, we have to reserve the file size
		// TODO: find way to grow file if necessary
		if (!this.blob.exists()) {
			this.blob.create(this.maxSize);
		}

		// check for rename if writing an additional message would cause max size to be exceeded
		this.pages = this.blob.downloadPageRanges();
		this.writeOffset = 0;
	}

	/*
	 * appends a log message to a blob
	 */
	public Boolean log(String message) throws Exception {

		this.setMessage(message);

		// initialise the blob (creates only if required
		initBlob();

		// check if rollover is required
		checkRollover();

		// get the write offset
		deriveWriteOffset();

		// update the blob
		appendToBlob();

		return true;
	}

	/*
	 * renames the blob
	 *
	 * @param blob - the blob to rename
	 */
	public Boolean rename() throws URISyntaxException, StorageException {
		CloudPageBlob newBlob = this.blobcontainer.getPageBlobReference(this.logFileName + this.fileSuffix);
		if (!newBlob.exists()) {
			newBlob.create(this.maxSize);
		}
		newBlob.startCopyFromBlob(this.blob);
		while (newBlob.getCopyState().getStatus() == CopyStatus.PENDING) {
			try {
				Thread.sleep(250);
			} catch (InterruptedException e) {
				return false;
			}
		}
		if (newBlob.getCopyState().getStatus() != CopyStatus.SUCCESS) {
			return false;
		} else {
			this.blob.delete();
		}
		this.fileSuffix += 1;
		return true;
	}

	public void setBlob(CloudPageBlob blob) {
		this.blob = blob;
	}

	public void setBlobcontainer(CloudBlobContainer blobcontainer) {
		this.blobcontainer = blobcontainer;
	}

	public void setFileSuffix(int fileSuffix) {
		this.fileSuffix = fileSuffix;
	}

	public void setLogFileName(String filename) throws Exception {
		if (filename == null || "".equals(filename)) {
			throw new Exception("Cannot write to unspecified file");
		}

		this.logFileName = filename;
	}

	public void setMaxSize(int maxSize) {
		this.maxSize = maxSize;
	}

	public void setMessage(String message) throws Exception {
		if (message == null) {
			throw new Exception("Cannot log null message");
		}
		// ensure the line ends with a carriage return
		if (message != null && !message.endsWith("\n")) {
			this.message = message + "\n";
		} else {
			this.message = message;
		}
	}
}

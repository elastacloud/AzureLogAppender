package com.elastacloud.spark.logger;

import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

import com.elastacloud.azure.blob.storage.PageBlobAppender;

/**
 * Created by david on 10/04/14.
 */
public class AzureBlobStorageLogger extends AppenderSkeleton {

	private String connection;
	private String container;
	private String fileSize;
	private String logFileName;
	private String logFileDatePattern;

	private PageBlobAppender appender;
	private Boolean connected = false;
	private Date currentDate = null;

	public AzureBlobStorageLogger() {

	}

	@Override
	protected void append(LoggingEvent loggingEvent) {
		loggingEvent.getMessage();

		try {

			if (this.connected == false) {
				this.connect();
			}

			if (this.appender == null) {
				throw new Exception("Cannot append to blob storage");
			}

			if (this.currentDate != null && hasDateChanged() || this.currentDate == null) {
				init();
			}

			this.appender.log(this.layout.format(loggingEvent));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void close() {

	}

	private void connect() {
		try {
			this.appender = new PageBlobAppender();
			this.appender.getBlobReference(this.getConnection(), this.getContainer());

		} catch (Exception e) {
			e.printStackTrace();
		}

		this.connected = true;
	}

	public String getConnection() {
		return this.connection;
	}

	public String getContainer() {
		return this.container;
	}

	private String getFilename(String pattern) throws UnknownHostException {

		String result = pattern;
		SimpleDateFormat sdf = new SimpleDateFormat(this.getLogFileDatePattern());
		result = result.replace("%d", sdf.format(this.currentDate));
		result = result.replace("%h", java.net.InetAddress.getLocalHost().getHostName());

		return result;
	}

	public String getFileSize() {
		return this.fileSize;
	}

	public String getLogFileDatePattern() {
		return this.logFileDatePattern;
	}

	public String getLogFileName() {
		return this.logFileName;
	}

	private Boolean hasDateChanged() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

		String d1 = sdf.format(this.currentDate);
		String d2 = sdf.format(new Date());

		if (d1.compareTo(d2) != 0) {
			return true;
		}

		return false;
	}

	private void init() throws Exception {

		this.currentDate = new Date();
		this.appender.setMaxSize(Integer.parseInt(this.getFileSize()));
		this.appender.setLogFileName(this.getFilename(this.getLogFileName()));
		this.appender.setFileSuffix(0);
	}

	public boolean requiresLayout() {

		return true;
	}

	public void setConnection(String connection) {
		this.connection = connection;
	}

	public void setContainer(String container) {
		this.container = container;
	}

	public void setFileSize(String fileSize) {
		this.fileSize = fileSize;
	}

	public void setLogFileDatePattern(String logFileDatePattern) {
		this.logFileDatePattern = logFileDatePattern;
	}

	public void setLogFileName(String logFileName) {
		this.logFileName = logFileName;
	}

}

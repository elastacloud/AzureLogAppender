package com.elastacloud.spark.logger;


import com.elastacloud.azure.blob.storage.PageBlobAppender;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

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

    public String getLogFileName() {
        return logFileName;
    }

    public void setLogFileName(String logFileName) {
        this.logFileName = logFileName;
    }

    public String getLogFileDatePattern() {
        return logFileDatePattern;
    }

    public void setLogFileDatePattern(String logFileDatePattern) {
        this.logFileDatePattern = logFileDatePattern;
    }

    public String getFileSize() {
        return fileSize;
    }

    public void setFileSize(String fileSize) {
        this.fileSize = fileSize;
    }

    public String getContainer() {
        return container;
    }

    public void setContainer(String container) {
        this.container = container;
    }

    public String getConnection() {
        return connection;
    }

    public void setConnection(String connection) {
        this.connection = connection;
    }

    private String getFilename(String pattern) throws UnknownHostException {

        String result = pattern;
        SimpleDateFormat sdf = new SimpleDateFormat(this.getLogFileDatePattern());
        result = result.replace("%d", sdf.format(currentDate));
        result = result.replace("%h", java.net.InetAddress.getLocalHost().getHostName());

        return result;
    }

    public AzureBlobStorageLogger()
    {

    }

    private void connect()
    {
        try {
            this.appender = new PageBlobAppender();
            this.appender.getBlobReference(this.getConnection(), this.getContainer());

        } catch (Exception e) {
            e.printStackTrace();
        }

        connected = true;
    }

    private void init() throws Exception {

        currentDate = new Date();
        appender.setMaxSize(Integer.parseInt(this.getFileSize()));
        appender.setLogFileName(this.getFilename(this.getLogFileName()));
        appender.setFileSuffix(0);
    }

    private Boolean hasDateChanged()
    {
       SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

       String d1 = sdf.format(currentDate);
       String d2 = sdf.format(new Date());

       if(d1.compareTo(d2) != 0)
           return true;

       return false;
    }
    @Override
    protected void append(LoggingEvent loggingEvent) {
        loggingEvent.getMessage();

        try {
            Map map = loggingEvent.getProperties();

            if(this.connected == false)
                this.connect();

           if(appender == null)
                throw new Exception("Cannot append to blob storage");

            if((currentDate != null && hasDateChanged()) || currentDate == null)
                init();

            appender.log(this.layout.format(loggingEvent));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean requiresLayout() {
        return true;
    }

    @Override
    public void close() {

    }
}

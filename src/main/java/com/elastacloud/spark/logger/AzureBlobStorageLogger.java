package com.elastacloud.spark.logger;


import com.elastacloud.azure.blob.storage.BlockBlobAppender;
import org.apache.log4j.*;
import org.apache.log4j.helpers.PatternParser;
import org.apache.log4j.pattern.PatternConverter;
import org.apache.log4j.spi.LoggingEvent;
import sun.util.logging.resources.logging;

import java.io.FileWriter;
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


    @Override
    protected void append(LoggingEvent loggingEvent) {
        loggingEvent.getMessage();

        try {
            Map map = loggingEvent.getProperties();

            SimpleDateFormat sdf = new SimpleDateFormat(this.getLogFileDatePattern());
            String logName = this.getLogFileName().replace("%d", sdf.format(new Date()));

            BlockBlobAppender appender = new BlockBlobAppender(connection, container, logName);;
            appender.setMaxSize(Integer.parseInt(fileSize));
            if(appender == null)
                throw new Exception("Cannot append to blob storage");

            appender.appendToFile(this.layout.format(loggingEvent));
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

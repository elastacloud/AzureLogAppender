AzureLogAppender
================

Description
==

A rolling log4j appender that writes log entries to a PageBlob in Azure Blob Storage

Log4j Configuration settings
==

log4j.rootCategory=INFO, blobappender
log4j.appender.blobappender=com.elastacloud.spark.logger.AzureBlobStorageLogger
log4j.appender.blobappender.connection=%connectionString%
log4j.appender.blobappender.container=%container%
log4j.appender.blobappender.fileSize=25600
log4j.appender.blobappender.logFileDatePattern=yyyy-MM-dd
log4j.appender.blobappender.logFileName=testlog-%h-%d.log
log4j.appender.blobappender.layout=org.apache.log4j.PatternLayout
log4j.appender.blobappender.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n

Replace 

<b>%connectionString%'</b> and '''%container%''' as appropriate
'''fileSize''' - is a configurable max file size in bytes for a log file NOTE: azure reserves the entire amount, when the logging of a message exceeds the max the file is automatically rolled over.
'''logFileDatePattern''' - date pattern for the log file name
'''logFileName''' - NOTE %h = hostname, %d = date formatted according to the pattern specified in logFileDatePattern

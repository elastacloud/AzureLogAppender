AzureLogAppender
================

Description
==

A rolling log4j appender that writes log entries to a PageBlob in Azure Blob Storage<br>

Log4j Configuration settings
==

log4j.rootCategory=INFO, blobappender<br>
log4j.appender.blobappender=com.elastacloud.spark.logger.AzureBlobStorageLogger<br>
log4j.appender.blobappender.connection=%connectionString%<br>
log4j.appender.blobappender.container=%container%<br>
log4j.appender.blobappender.fileSize=25600<br>
log4j.appender.blobappender.logFileDatePattern=yyyy-MM-dd<br>
log4j.appender.blobappender.logFileName=testlog-%h-%d.log<br>
log4j.appender.blobappender.layout=org.apache.log4j.PatternLayout<br>
log4j.appender.blobappender.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n<br>
<br>
Replace<br> 
<br>
<b>%connectionString%</b> and <b>%container%</b> as appropriate<br>
<b>fileSize</b> - is a configurable max file size in bytes for a log file NOTE: azure reserves the entire amount, when the logging of a message exceeds the max the file is automatically rolled over.<br>
<b>logFileDatePattern</b> - date pattern for the log file name<br>
<b>logFileName</b> - NOTE %h = hostname, %d = date formatted according to the pattern specified in logFileDatePattern<br>

package com.elastacloud.azure.blob.storage;

import com.microsoft.windowsazure.services.blob.client.*;
import com.microsoft.windowsazure.services.core.storage.CloudStorageAccount;
import com.microsoft.windowsazure.services.core.storage.StorageException;
import org.apache.commons.lang3.ArrayUtils;

import java.io.*;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

/**
 * Created by david on 10/04/14.
 */
public class PageBlobAppender {



    private CloudBlobContainer blobcontainer = null;
    private String logFileName = "";
    private int maxSize  = 0;
    private final int PAGE_SIZE_MULTIPLE = 512;

    private int fileSuffix = 0;
    private List<PageRange> pages = null;

    private CloudStorageAccount storageAccount = null;



    private CloudPageBlob blob = null;
    private CloudBlobClient blobClient = null;

    private String message = null;

    private long writeOffset = 0;

    public void setMaxSize(int maxSize) {
        this.maxSize = maxSize;
    }

    /*
    Constructor
     */
    public PageBlobAppender()
    {

    }

    public PageBlobAppender(CloudStorageAccount storageAccount)
    {
        this.storageAccount = storageAccount;
    }

    public void getBlobReference(String connectionString, String containerName) throws Exception
    {
        this.storageAccount = CloudStorageAccount.parse(connectionString);
        this.blobClient = storageAccount.createCloudBlobClient();
        this.blobcontainer = blobClient.getContainerReference(containerName);
    }

    /*
    Returns a new instance of the source aligned to 512 bytes
    @param source - the byte array source
     */
    private byte[] getAlignedBuffer(byte[] source)
    {
        int remainder = source.length%PAGE_SIZE_MULTIPLE;
        int size = source.length;
        if(remainder > 0)
            size += PAGE_SIZE_MULTIPLE - remainder;

        byte[] buffer = Arrays.copyOf(ArrayUtils.EMPTY_BYTE_ARRAY, size);
        Arrays.fill(buffer, (byte)0);
        System.arraycopy(source, 0, buffer, 0, source.length);

        return buffer;
    }

    /*
    Determines whether log file rollover is required based on the current last written point and the length of hte message in bytes
    @param fileSize - last written location in the file
    @param currentMessage - the current message to write
     */
    private void checkRollover() throws URISyntaxException, StorageException {

        if(pages != null && pages.size() > 0 && (pages.get(pages.size()-1).getEndOffset() + this.message.getBytes().length) > maxSize)
        {
            //rename
            rename();

            //recreate log file
            initBlob();
        }
    }

    private void deriveWriteOffset() throws StorageException {
        //check to see if we can append to the current page, if a page exists
        if(pages.size() > 0)
        {
            //get the most recent page
            PageRange lastPage = pages.get(pages.size()-1);

            //get the last page and download the last 512 bytes
            byte[] buf = new byte[PAGE_SIZE_MULTIPLE];
            blob.downloadRange(lastPage.getEndOffset() -PAGE_SIZE_MULTIPLE+1, PAGE_SIZE_MULTIPLE, buf, 0);

            //find the first null reference in the file
            String str = new String(buf);
            int index = str.indexOf((byte) 0);

            //we have found a null
            if(index > 0)
            {
                //strip off any null characters
                String subStr = str.substring(0, str.indexOf((byte)0));

                //append the message
                this.message = subStr + message;

                //set the write offset to be the start of the 512 bytes we downloaded
                this.writeOffset =  lastPage.getEndOffset() -PAGE_SIZE_MULTIPLE+1;
            }
            else
            {
                //otherwise write at the end of the last 512 bytes
                this.writeOffset =  lastPage.getEndOffset() +1;
            }
        }
    }

    /*
    renames the blob
    @param blob - the blob to rename
     */
    public Boolean rename() throws URISyntaxException, StorageException {
        CloudPageBlob newBlob = blobcontainer.getPageBlobReference(this.logFileName + fileSuffix);
        if(!newBlob.exists())
            newBlob.create(maxSize);
        newBlob.copyFromBlob(this.blob);
        blob.delete();
        fileSuffix +=1;
        return true;
    }

    private void initBlob() throws URISyntaxException, StorageException {
        //get a reference to the blob
        this.blob = blobcontainer.getPageBlobReference(this.logFileName);

        //create if not exists, we have to reserve the file size
        //TODO: find way to grow file if necessary
        if(!blob.exists())
            blob.create(this.maxSize);

        //check for rename if writing an additional message would cause max size to be exceeded
        this.pages = blob.downloadPageRanges();
        this.writeOffset = 0;
    }

    private void appendToBlob() throws IOException, StorageException {
        //get a stream
        byte[] buffer = getAlignedBuffer(this.message.getBytes());
        InputStream inStream = new ByteArrayInputStream(buffer);

        //write the data
        blob.uploadPages(inStream, this.writeOffset, (long) buffer.length);
    }
    /*
    appends a log message to a blob
     */
    public Boolean log(String message) throws Exception {

        this.setMessage(message);

        //initialise the blob (creates only if required
        initBlob();

        //check if rollover is required
        checkRollover();

        //get the write offset
        deriveWriteOffset();

        //update the blob
        appendToBlob();

        return true;
    }

    public void setMessage(String message) throws Exception {
        if(message == null)
            throw new Exception("Cannot log null message");
        //ensure the line ends with a carriage return
        if(message != null && !message.endsWith("\n"))
            this.message = message + "\n";
        else
            this.message = message;
    }

    public void setLogFileName(String filename) throws Exception {
        if(filename == null || "".equals(filename))
            throw new Exception("Cannot write to unspecified file");

        this.logFileName = filename;
    }

    public void setFileSuffix(int fileSuffix) {
        this.fileSuffix = fileSuffix;
    }

    public void setBlobcontainer(CloudBlobContainer blobcontainer) {
        this.blobcontainer = blobcontainer;
    }

    public void setBlob(CloudPageBlob blob) {
        this.blob = blob;
    }
}

package com.elastacloud.azure.blob.storage;

import com.microsoft.windowsazure.services.blob.client.*;
import com.microsoft.windowsazure.services.blob.models.ListBlobsResult;
import com.microsoft.windowsazure.services.core.storage.AccessCondition;
import com.microsoft.windowsazure.services.core.storage.CloudStorageAccount;
import com.microsoft.windowsazure.services.core.storage.StorageException;
import com.microsoft.windowsazure.services.core.storage.utils.Base64;
import org.apache.commons.lang3.ArrayUtils;
import sun.org.mozilla.javascript.internal.ast.Block;

import java.io.*;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * Created by david on 10/04/14.
 */
public class BlockBlobAppender {

    private CloudBlobContainer blobcontainer = null;
    private String targetFile = "";
    private int maxSize  = 0;
    private final int PAGE_SIZE_MULTIPLE = 512;

    public void setMaxSize(int maxSize) {
        this.maxSize = maxSize;
    }

    public BlockBlobAppender(String connectionString, String containerName, String targetFile) throws Exception
    {
        if(targetFile == null || "".equals(targetFile))
            throw new Exception("Cannot write to unspecified file");

        CloudStorageAccount storageAccount = CloudStorageAccount.parse(connectionString);

        CloudBlobClient blobClient = storageAccount.createCloudBlobClient();

        this.blobcontainer = blobClient.getContainerReference(containerName);
        this.targetFile = targetFile;
    }

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

    public Boolean appendToFile(String message) throws Exception {

        long writeOffset = 0;
        if(message == null)
            throw new Exception("Cannot log null message");

        //get a reference to the blob
        CloudPageBlob blob = blobcontainer.getPageBlobReference(this.targetFile);

        //create if not exists, we have to reserve the file size
        //TODO: find way to grow file if necessary
        if(!blob.exists())
            blob.create(maxSize);

        //ensure the line ends with a carriage return
        if(message != null && !message.endsWith("\n"))
            message += "\n";

        //check to see if we can append to the current page
        List<PageRange> pages = blob.downloadPageRanges();
        long currentSize = 0;

        //if we have pages
        if(pages.size() > 0)
        {
            //get the most recent page
            PageRange lastPage = pages.get(pages.size()-1);

            //get the last page
            int lastPageLength = (int)(lastPage.getEndOffset()-lastPage.getStartOffset());
            currentSize = lastPage.getEndOffset() -PAGE_SIZE_MULTIPLE;
            byte[] buf = new byte[PAGE_SIZE_MULTIPLE];

            //download the pas 512 bytes only
            blob.downloadRange(lastPage.getEndOffset() -PAGE_SIZE_MULTIPLE+1, PAGE_SIZE_MULTIPLE, buf, 0);

            String str = new String(buf);
            int index = str.indexOf((byte) 0);

            if(index > 0)
            {
                String subStr = str.substring(0, str.indexOf((byte)0));

                message = subStr + message;
                writeOffset =  lastPage.getEndOffset() -PAGE_SIZE_MULTIPLE+1;
            }
            else
            {
                writeOffset =  lastPage.getEndOffset() +1;
            }
        }

        byte[] buffer = getAlignedBuffer(message.getBytes());
        InputStream inStream = new ByteArrayInputStream(buffer);

        blob.uploadPages(inStream, writeOffset, (long) buffer.length);

        return true;
    }
}

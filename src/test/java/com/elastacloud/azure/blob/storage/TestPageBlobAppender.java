package com.elastacloud.azure.blob.storage;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.URISyntaxException;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudPageBlob;
import com.microsoft.azure.storage.blob.CopyState;

/**
 * Created by david on 14/04/14.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ CloudStorageAccount.class, CloudBlobClient.class, CloudBlobContainer.class, CloudPageBlob.class })
public class TestPageBlobAppender {

	@Test
	public void TestPageBlobAppender_Test_BufferIsAligned() throws URISyntaxException, StorageException {
		PowerMockito.spy(PageBlobAppender.class);
		// PowerMockito.doCallRealMethod().when( )

	}

	@Test
	public void TestPageBlobAppender_TestRename() throws URISyntaxException, StorageException {
		PowerMockito.spy(CloudPageBlob.class);

		CloudPageBlob oldBlob = PowerMockito.mock(CloudPageBlob.class);
		CloudPageBlob newBlob = PowerMockito.mock(CloudPageBlob.class);
		CloudBlobContainer container = mock(CloudBlobContainer.class);
		CloudBlobClient client = mock(CloudBlobClient.class);
		CloudStorageAccount storageAccount = mock(CloudStorageAccount.class);
		// FIXME can't mock final class
		// CopyState copyState = mock(CopyState.class);
		CopyState copyState = new CopyState();
		// copyState.setStatus(CopyStatus.SUCCESS);

		when(storageAccount.createCloudBlobClient()).thenReturn(client);
		when(client.getContainerReference("container")).thenReturn(container);
		when(container.getPageBlobReference("testLog.log")).thenReturn(oldBlob);
		when(container.getPageBlobReference("testLog.log0")).thenReturn(newBlob);

		PowerMockito.when(newBlob.exists()).thenReturn(false);
		doNothing().when(newBlob).create(1024);
		when(newBlob.startCopyFromBlob(oldBlob)).thenReturn("");
		when(newBlob.getCopyState()).thenReturn(copyState);
		doNothing().when(oldBlob).delete();

		try {
			PageBlobAppender appender = new PageBlobAppender(storageAccount);
			appender.setBlobcontainer(container);
			appender.setLogFileName("testLog.log");
			appender.setMaxSize(1024);
			appender.setBlob(oldBlob);
			// FIXME find way to mock copystate
			// appender.rename();

		} catch (Exception ex) {
			Assert.fail("Expected no exception, but got: " + ex.getMessage());
		}

		// FIXME mocking from above
		// verify(container, times(1)).getPageBlobReference("testLog.log0");
		// verify(newBlob, times(1)).exists();
		// verify(newBlob, times(1)).startCopyFromBlob(oldBlob);
		// verify(oldBlob, times(1)).delete();
	}
}

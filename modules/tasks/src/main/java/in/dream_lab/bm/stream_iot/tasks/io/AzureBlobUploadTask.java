package in.dream_lab.bm.stream_iot.tasks.io;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Uploads a local file to blob on Azure cloud
 * 
 * This task is thread-safe, and can be run from multiple threads. 
 * 
 * @author shukla, simmhan
 *
 */
public class AzureBlobUploadTask extends AbstractTask {

	private static final Object SETUP_LOCK = new Object();
	// static fields common to all threads
	private static boolean doneSetup = false;
	private static int useMsgField;
	
	private static String storageConnStr;
	private static String containerName;
	private  String[] localFilePaths ;
	
	public void setup(Logger l_, Properties p_) {
		super.setup(l_, p_);
		synchronized (SETUP_LOCK) {
			if(!doneSetup) { // Do setup only once for this task
				// If positive, use it for index over file names else read randomly
				useMsgField = Integer.parseInt(p_.getProperty("IO.AZURE_BLOB.USE_MSG_FIELD"));
				
				storageConnStr = p_.getProperty("IO.AZURE_STORAGE_CONN_STR");
				containerName = p_.getProperty("IO.AZURE_BLOB.CONTAINER_NAME");

				doneSetup=true;
			}

			String csvLocalPaths = p_.getProperty("IO.AZURE_BLOB_UPLOAD.FILE_SOURCE_PATH");
			l.info("csvLocalPaths"+csvLocalPaths);
			assert csvLocalPaths != null;
			localFilePaths = csvLocalPaths.split(",");
		}
	}
	
	@Override
	protected Float doTaskLogic(String m) {
		//pass file index to be downloaded in message or at random
		if(l.isInfoEnabled())
			l.info("localFilePaths"+ Arrays.toString(localFilePaths));

		int localFileSourcePathIndex;
		if(useMsgField>0){
			localFileSourcePathIndex= Integer.parseInt(m.split(",")[useMsgField-1]) % localFilePaths.length;
		}
		else {
			localFileSourcePathIndex = ThreadLocalRandom.current().nextInt(localFilePaths.length);
		}
		
		String localFileSourcePath = localFilePaths[localFileSourcePathIndex];
		CloudBlobContainer container = connectToAzContainer(storageConnStr, containerName,  l);
		assert container != null;
		int result = putAzBlob(container,localFileSourcePath,l);
		return Float.valueOf(result);
	}


	/***
	 *
	 * @param azStorageConnStr
	 * @param containerName
	 * @param l
     * @return
     */
	public static CloudBlobContainer connectToAzContainer(String azStorageConnStr, String containerName, Logger l) {
		try {
			// Retrieve storage account from connection-string.
			CloudStorageAccount storageAccount = CloudStorageAccount.parse(azStorageConnStr);

			// Create the blob client.
			CloudBlobClient blobClient = storageAccount.createCloudBlobClient();

			// Retrieve reference to a previously created container.
			CloudBlobContainer container = blobClient.getContainerReference(containerName);

			return container;
		} catch (Exception e) {
			l.warn("Exception in connectToAzContainer: "+containerName+"\n"+ e);
		}

		return null;
	}

	/***
	 * @param container
	 * @param localPath
	 * @param l  @return
     */

	public static int putAzBlob(CloudBlobContainer container, String localPath, Logger l) {
		try {
			String fileName = Paths.get(localPath).getFileName().toString();

			if(l.isInfoEnabled())
				l.info("Uploding File .... File name is {} from local file path : {} ", fileName, localPath);

			CloudBlockBlob blob = container.getBlockBlobReference(fileName);
			File source = new File(localPath);
			blob.upload(new FileInputStream(source), source.length());

			return  1;
		} catch (Exception e) {
			l.warn("Exception in getAzBlob: "+container);
		}
		return -1;
	}
}

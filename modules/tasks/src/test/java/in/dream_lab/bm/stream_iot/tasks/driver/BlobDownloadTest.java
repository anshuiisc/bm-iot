package in.dream_lab.bm.stream_iot.tasks.driver;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import in.dream_lab.bm.stream_iot.tasks.io.AzureBlobDownloadTask;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.assertTrue;

/**
 * Created by anshushukla on 27/05/16.
 */
public class BlobDownloadTest extends AzureBlobDownloadTask {

//    Float size;
    private static Logger l; // TODO: Ensure logger is initialized before use

    /**
     *
     * @param l_
     */
    public static void initLogger(Logger l_) {
        l = l_;
    }

    public static void main(String[] args) {


        AzureBlobDownloadTask az_blob=new AzureBlobDownloadTask();

        initLogger(LoggerFactory.getLogger("APP"));
        Properties p_=new Properties();

        try {
            p_.load(new FileReader("src/main/resources/tasks.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("blob test is working ********** "+p_);

        az_blob.setup(l,p_ );
        String fileIndex="5,1443033000,ci4ue1845000102w7ni64j7pl,-71.106167,42.372802,-0.1,65.3,0,367.38,26";
        Float size = az_blob.doTask(fileIndex);
//        Assert.assertEquals(az_blob.doTask(fileIndex), "Hello World");

        az_blob.tearDown();


    }

    @Test
    public void testBlob()
    {
        AzureBlobDownloadTask az_blob=new AzureBlobDownloadTask();

        initLogger(LoggerFactory.getLogger("APP"));
        Properties p_=new Properties();
        System.out.println("blob test is working ********** ");
        try {
            p_.load(new FileReader("src/main/resources/tasks.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        az_blob.setup(l,p_ );
        String fileIndex="5,1443033000,ci4ue1845000102w7ni64j7pl,-71.106167,42.372802,-0.1,65.3,0,367.38,26";
        Float size = az_blob.doTask(fileIndex);
        System.out.println("Size of blob is "+size);

        az_blob.tearDown();
        assertTrue(size>0);
    }
}


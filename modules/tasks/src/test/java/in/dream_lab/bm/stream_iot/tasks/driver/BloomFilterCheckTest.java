package in.dream_lab.bm.stream_iot.tasks.driver;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import in.dream_lab.bm.stream_iot.tasks.filter.BloomFilterCheck;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by anshushukla on 27/05/16.
 */
public class BloomFilterCheckTest extends BloomFilterCheck {


    private static Logger l; // TODO: Ensure logger is initialized before use

    /**
     *
     * @param l_
     */
    public static void initLogger(Logger l_) {
        l = l_;
    }

    public static void main(String[] args) {


        BloomFilterCheck l1=new BloomFilterCheck();

        initLogger(LoggerFactory.getLogger("APP"));
        Properties p_=new Properties();
        try {
            p_.load(new FileReader("src/main/resources/tasks.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }


        l1.setup(l,p_ );
        String m="1,1443033000,ci4ue1845000102w7ni64j7pl,-71.106167,42.372802,-0.1,65.3,0,367.38,26";
        l1.doTask(m);

        // to check false
        String m1="11,1443033000,ci114ue1845000102w7ni64j7pl,-71.106167,42.372802,-0.1,65.3,0,367.38,26";
        l1.doTask(m1);
        l.warn(String.valueOf(l1.tearDown()));
    }

    @Test
    public void testBloomFilterCheckTest()
    {
        BloomFilterCheck l1=new BloomFilterCheck();

        initLogger(LoggerFactory.getLogger("APP"));
        Properties p_=new Properties();

        try {
            p_.load(new FileReader("src/main/resources/tasks.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        l1.setup(l,p_ );
        String m="1,1443033000,ci4ue1845000102w7ni64j7pl,-71.106167,42.372802,-0.1,65.3,0,367.38,26";

        System.out.println(String.valueOf(l1.doTask(m)));

        // to check false
        String m1="11,1443033000,ci114ue1845000102w7ni64j7pl,-71.106167,42.372802,-0.1,65.3,0,367.38,26";
        System.out.println(l1.doTask(m1));
        l.warn(String.valueOf(l1.tearDown()));

    }
}

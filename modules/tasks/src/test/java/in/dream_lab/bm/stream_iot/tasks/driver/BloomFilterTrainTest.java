package in.dream_lab.bm.stream_iot.tasks.driver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import in.dream_lab.bm.stream_iot.tasks.filter.BloomFilterTrain;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by anshushukla on 27/05/16.
 */
public class BloomFilterTrainTest extends BloomFilterTrain {


    private static Logger l; // TODO: Ensure logger is initialized before use

    /**
     *
     * @param l_
     */
    public static void initLogger(Logger l_) {
        l = l_;
    }

    public static void main(String[] args) {

        BloomFilterTrain bloomFilterTrain=new BloomFilterTrain();
        initLogger(LoggerFactory.getLogger("APP"));
        Properties p_=new Properties();
        try {
            p_.load(new FileReader("/Users/anshushukla/Downloads/Incomplete/stream/iot-bm/modules/tasks/src/main/resources/tasks.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        bloomFilterTrain.setup(l,p_ );
//        msgId,timestamp,source,longitude,latitude,temperature,humidity,light,dust,airquality_raw
//        1,1443033000,ci4ue1845000102w7ni64j7pl,-71.106167,42.372802,-0.1,65.3,0,367.38,26


        String m="1,1443033000,ci4ue1845000102w7ni64j7pl,-71.106167,42.372802,-0.1,65.3,0,367.38,26";
        bloomFilterTrain.doTask(m);
        m="2,1443033000,ci4vjer3i000e02s7r2cj23gs,-43.1833012,-22.913566699999997,31.5,58.3,0,239.96,27";
        bloomFilterTrain.doTask(m);

        l.warn(String.valueOf(bloomFilterTrain.tearDown()));
    }

//    @Test
//    public void testBloomFilterTrainTest()
//    {
//
//        BloomFilterTrain bloomFilterTrain=new BloomFilterTrain();
//
//        initLogger(LoggerFactory.getLogger("APP"));
//        Properties p_=new Properties();
//
//
//        bloomFilterTrain.setup(l,p_ );
////        msgId,timestamp,source,longitude,latitude,temperature,humidity,light,dust,airquality_raw
////        1,1443033000,ci4ue1845000102w7ni64j7pl,-71.106167,42.372802,-0.1,65.3,0,367.38,26
//
//
//        String m="1,1443033000,ci4ue1845000102w7ni64j7pl,-71.106167,42.372802,-0.1,65.3,0,367.38,26";
//        bloomFilterTrain.doTask(m);
//        m="2,1443033000,ci4vjer3i000e02s7r2cj23gs,-43.1833012,-22.913566699999997,31.5,58.3,0,239.96,27";
//        bloomFilterTrain.doTask(m);
//
//        l.warn(String.valueOf(bloomFilterTrain.tearDown()));
////        assertEquals(0,new App().calculateSomething());
//    }
}

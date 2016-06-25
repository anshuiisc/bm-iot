package in.dream_lab.bm.stream_iot.tasks.driver;

import in.dream_lab.bm.stream_iot.tasks.predict.LinearRegressionTrain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by anshushukla on 27/05/16.
 */
public class LinearRegressionTrainTest extends LinearRegressionTrain {


    private static Logger l; // TODO: Ensure logger is initialized before use

    /**
     *
     * @param l_
     */
    public static void initLogger(Logger l_) {
        l = l_;
    }

    public static void main(String[] args) {


        LinearRegressionTrain l1=new LinearRegressionTrain();

        initLogger(LoggerFactory.getLogger("APP"));
        Properties p_=new Properties();
        try {
            p_.load(new FileReader("/Users/anshushukla/Downloads/Incomplete/stream/iot-bm/modules/tasks/src/main/resources/tasks.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        l1.setup(l,p_ );

//        String input_SYSrow="-71.106167,42.372802,-0.1,65.3,0,367.38,26";
//        l1.doTask(input_SYSrow);

        for(int c=0;c<5000;c++) {// Dummy data to see changes in model
//            l1.doTask( "-0.1,65.3,0,367.38,26,Good");
            l1.doTask("11.1,11165.3,23");
        }

        l1.tearDown();


    }

//    @Test
//    public void testLinearRegressionTrainTest()
//    {
//        LinearRegressionTrain l1=new LinearRegressionTrain();
//
//        initLogger(LoggerFactory.getLogger("APP"));
//        Properties p_=new Properties();
//        try {
//            p_.load(new FileReader("src/main/resources/tasks.properties"));
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//
//        l1.setup(l,p_ );
//
////        String input_SYSrow="-71.106167,42.372802,-0.1,65.3,0,367.38,26";
////        l1.doTask(input_SYSrow);
//
//        for(int c=0;c<5000;c++) {// Dummy data to see changes in model
//            Float trainSuccess= l1.doTask("-71.106167," + c * 42.372802 + ",-0.1,65.3,0,367.38,26,Good");
//            Assert.assertTrue(trainSuccess==0); // train Success flag as return
//            trainSuccess=l1.doTask("-711111.106167,"+(c*4.372802)+",1110.1,11165.3,0,"+(367.38+c)+",26000,BAD");
//            Assert.assertTrue(trainSuccess==0); // train Success flag as return
//
//        }
//
//        l1.tearDown();
//
//    }
}

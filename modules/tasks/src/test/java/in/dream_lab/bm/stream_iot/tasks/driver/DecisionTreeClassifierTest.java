package in.dream_lab.bm.stream_iot.tasks.driver;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import in.dream_lab.bm.stream_iot.tasks.predict.DecisionTreeClassify;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.assertTrue;

/**
 * Created by anshushukla on 27/05/16.
 */
public class DecisionTreeClassifierTest extends DecisionTreeClassify {


    private static Logger l; // TODO: Ensure logger is initialized before use

    /**
     *
     * @param l_
     */
    public static void initLogger(Logger l_) {
        l = l_;
    }

    public static void main(String[] args) {


        DecisionTreeClassify l1=new DecisionTreeClassify();

        initLogger(LoggerFactory.getLogger("APP"));
        Properties p_=new Properties();
        try {
            p_.load(new FileReader("/Users/anshushukla/Downloads/Incomplete/stream/iot-bm/modules/tasks/src/main/resources/tasks.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }


        l1.setup(l,p_ );
//        for(int c=0;c<20;c++)

        // for SYS dataset
//        System.out.println(l1.doTask("-0.127625,51.503363,7.8,36.7,0,557.43,15"));


        // for TAXI dataset
        System.out.println(l1.doTask("111111420,1.95,8.00"));

        l.warn(String.valueOf(l1.tearDown()));


    }

    @Test
    public void testDecisionTreeClassifierTest()
    {
        DecisionTreeClassify l1=new DecisionTreeClassify();

        initLogger(LoggerFactory.getLogger("APP"));
        Properties p_=new Properties();

        try {
            p_.load(new FileReader("src/main/resources/tasks.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        l1.setup(l,p_ );
//        for(int c=0;c<20;c++)
        Float enumIndex = l1.doTask("-0.127625,51.503363,7.8,36.7,0,557.43,15");

        System.out.println("enumIndex"+enumIndex);
        assertTrue(enumIndex>=0); // index fo result in enum can never be negative
//        assertEquals(0,new App().calculateSomething());

        System.out.println(String.valueOf(l1.tearDown()));
    }
}

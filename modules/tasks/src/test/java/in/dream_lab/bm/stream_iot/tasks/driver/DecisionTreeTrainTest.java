package in.dream_lab.bm.stream_iot.tasks.driver;


import com.google.common.base.Charsets;
import com.google.common.io.Files;
import in.dream_lab.bm.stream_iot.tasks.predict.DecisionTreeTrain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * Created by anshushukla on 27/05/16.
 */
public class DecisionTreeTrainTest extends DecisionTreeTrain {


    private static Logger l; // TODO: Ensure logger is initialized before use

    /**
     * @param l_
     */
    public static void initLogger(Logger l_) {
        l = l_;
    }

    public static void main(String[] args) {


        DecisionTreeTrain decisionTreeTrain = new DecisionTreeTrain();

        initLogger(LoggerFactory.getLogger("APP"));
        Properties p_ = new Properties();
        try {
            p_.load(new FileReader("/Users/anshushukla/Downloads/Incomplete/stream/iot-bm/modules/tasks/src/main/resources/tasks.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        decisionTreeTrain.setup(l, p_);

//        String dummy="-71.106167,42.372802,-0.1,65.3,0,367.38,26,Good";
//        for (int c = 0; c < 1150; c++) {


        List<String> lines = null;
        try {
//             lines = Files.readLines(new File("/Users/anshushukla/Downloads/Incomplete/stream/iot-bm/modules/tasks/src/test/java/in/dream_lab/bm/stream_iot/tasks/driver/test-decision-SYS.txt"), Charsets.UTF_8);
            lines = Files.readLines(new File("/Users/anshushukla/Downloads/Incomplete/stream/iot-bm/modules/tasks/src/test/java/in/dream_lab/bm/stream_iot/tasks/driver/test-decision-TAXI.txt"), Charsets.UTF_8);
        } catch (IOException e) {
            e.printStackTrace();
        }


        StringBuilder lineToTrain = new StringBuilder();
                for (int c = 0; c < 1150; c++) {
                    for (String s : lines)


                    {
                        String[] split = s.split(",");

//for SYS
//                        for(int k=4;k<split.length;k++) {
//                            lineToTrain.append(split[k]);
//                            lineToTrain.append(",");
//                        }

//for taxi


//                            lineToTrain.append(split[3]).append(","); // ts
                            lineToTrain.append(split[4]).append(","); // triptimeInSecs
                            lineToTrain.append(split[5]).append(","); // tripDistance
                            lineToTrain.append(split[11]).append(","); // fareAmount
                            lineToTrain.append(split[17]).append(","); // fareAmount





                            System.out.println("line-" + lineToTrain);
//            decisionTreeTrain.doTask("-71.106167,42.372802,  -0.1,   65.3,  0,"+(367.38+c*20)+",26,Good");
//            decisionTreeTrain.doTask("103.792581,1.302858,  27.6,  69.7,  40,  507.35,        23,VeryGood");
                        decisionTreeTrain.doTask(lineToTrain.toString());

                        lineToTrain.setLength(0);
                    }
                }

        l.warn(String.valueOf(decisionTreeTrain.tearDown()));


    }

//    @Test
//    public void testDecisionTreeTrainTest() throws IOException {
//        DecisionTreeTrain decisionTreeTrain = new DecisionTreeTrain();
//
//        initLogger(LoggerFactory.getLogger("APP"));
//        Properties p_ = new Properties();
//
//        try {
//            p_.load(new FileReader("src/main/resources/tasks.properties"));
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        decisionTreeTrain.setup(l, p_);
//
////        String dummy="-71.106167,42.372802,-0.1,65.3,0,367.38,26,Good";
//
////        for(int c=0;c<1150;c++) {
//
//
//        List<String> lines = Files.readLines(new File("/Users/anshushukla/Downloads/Incomplete/stream/iot-bm/modules/tasks/src/test/java/in/dream_lab/bm/stream_iot/tasks/driver/test-decision-SYS.txt"), Charsets.UTF_8);
//
//
//        for (String s : lines){
//            System.out.println(lines);
//        }
//        Float trainSuccess = decisionTreeTrain.doTask("-71.106167,42.372802,-0.1,65.3,0," + (367.38 * 20) + ",26,Good");
//        l.warn("trainSuccess-" + trainSuccess);
////            Assert.assertTrue(trainSuccess>=0); // train Success flag as return
//
////            trainSuccess =decisionTreeTrain.doTask("-71.106167,42.372802,-0.1,65.3,0,"+(367.38+c)+",2600,Bad");
////            Assert.assertTrue(trainSuccess>=0); // train Success flag as return
//    }
//
////        l.warn(String.valueOf(decisionTreeTrain.tearDown()));


   }



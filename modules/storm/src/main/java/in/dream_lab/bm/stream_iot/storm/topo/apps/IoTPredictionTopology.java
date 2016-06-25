package in.dream_lab.bm.stream_iot.storm.topo.apps;

/**
 * Created by anshushukla on 03/06/16.
 */


import in.dream_lab.bm.stream_iot.storm.bolts.IoTPredictionBolts.SYS.AzureBlobUploadTaskBolt;
import in.dream_lab.bm.stream_iot.storm.bolts.IoTPredictionBolts.SYS.AzureTableTaskBolt;
import in.dream_lab.bm.stream_iot.storm.bolts.IoTPredictionBolts.SYS.DecisionTreeClassifyBolt;
import in.dream_lab.bm.stream_iot.storm.bolts.IoTPredictionBolts.SYS.LinearRegressionPredictorBolt;
import in.dream_lab.bm.stream_iot.storm.genevents.factory.ArgumentClass;
import in.dream_lab.bm.stream_iot.storm.genevents.factory.ArgumentParser;
import in.dream_lab.bm.stream_iot.storm.sinks.Sink;
import in.dream_lab.bm.stream_iot.storm.spouts.SampleSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;


/**
 * Created by anshushukla on 18/05/15.
 */
public class IoTPredictionTopology {

    public static void main(String[] args) throws Exception {

        ArgumentClass argumentClass = ArgumentParser.parserCLI(args);
        if (argumentClass == null) {
            System.out.println("ERROR! INVALID NUMBER OF ARGUMENTS");
            return;
        }

        String logFilePrefix = argumentClass.getTopoName() + "-" + argumentClass.getExperiRunId() + "-" + argumentClass.getScalingFactor() + ".log";
        String sinkLogFileName = argumentClass.getOutputDirName() + "/sink-" + logFilePrefix;
        String spoutLogFileName = argumentClass.getOutputDirName() + "/spout-" + logFilePrefix;
        String taskPropFilename=argumentClass.getTasksPropertiesFilename();
        System.out.println("taskPropFilename-"+taskPropFilename);


        Config conf = new Config();
        conf.setDebug(false);
        //conf.setNumWorkers(12);


        Properties p_=new Properties();
        InputStream input = new FileInputStream(taskPropFilename);
        p_.load(input);



        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new SampleSpout(argumentClass.getInputDatasetPathName(), spoutLogFileName, argumentClass.getScalingFactor()),
                1);

//        builder.setBolt("XMLParseBolt",
//                new XMLParsePredictBolt(p_), 1)
//                .shuffleGrouping("spout");

        builder.setBolt("AzureTableTaskBolt",
                new AzureTableTaskBolt(p_), 1)
                .shuffleGrouping("XMLParseBolt");

        builder.setBolt("LinearRegressionPredictorBolt",
                new LinearRegressionPredictorBolt(p_), 1)
                .shuffleGrouping("AzureTableTaskBolt");

        builder.setBolt("DecisionTreeClassifyBolt",
                new DecisionTreeClassifyBolt(p_), 1)
                .shuffleGrouping("AzureTableTaskBolt");

        builder.setBolt("AzureBlobUploadTaskBolt",
                new AzureBlobUploadTaskBolt(p_), 1)
                .shuffleGrouping("LinearRegressionPredictorBolt").shuffleGrouping("DecisionTreeClassifyBolt");

        builder.setBolt("sink", new Sink(sinkLogFileName), 1).shuffleGrouping("AzureBlobUploadTaskBolt");



        StormTopology stormTopology = builder.createTopology();

        if (argumentClass.getDeploymentMode().equals("C")) {
            StormSubmitter.submitTopology(argumentClass.getTopoName(), conf, stormTopology);
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(argumentClass.getTopoName(), conf, stormTopology);
            Utils.sleep(100000);
            cluster.killTopology(argumentClass.getTopoName());
            cluster.shutdown();
        }
    }
}


//    L   IdentityTopology   /Users/anshushukla/PycharmProjects/DataAnlytics1/Storm-Scheduler-SC-scripts/SYS-inputcsvSCTable-1spouts100mps-480sec.csv     PLUG-210  1.0   /Users/anshushukla/data/output/temp    /Users/anshushukla/Downloads/Incomplete/stream/iot-bm/modules/tasks/src/main/resources/tasks.properties  test
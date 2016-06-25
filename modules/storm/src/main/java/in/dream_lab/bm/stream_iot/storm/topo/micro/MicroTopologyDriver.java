package in.dream_lab.bm.stream_iot.storm.topo.micro;

import in.dream_lab.bm.stream_iot.storm.bolts.BaseTaskBolt;
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
public class MicroTopologyDriver {

	public static void main(String[] args) throws Exception {

		ArgumentClass argumentClass = ArgumentParser.parserCLI(args);
		if (argumentClass == null) {
			System.out.println("ERROR! INVALID NUMBER OF ARGUMENTS");
			return;
		}

		String logFilePrefix = argumentClass.getTopoName() + "-" + argumentClass.getExperiRunId() + "-"
				+ argumentClass.getScalingFactor() + ".log";
		String sinkLogFileName = argumentClass.getOutputDirName() + "/sink-" + logFilePrefix;
		String spoutLogFileName = argumentClass.getOutputDirName() + "/spout-" + logFilePrefix;
		String taskPropFilename = argumentClass.getTasksPropertiesFilename();

		Config conf = new Config();
		conf.setDebug(true); // make it false for actual benchmark
//		conf.put("topology.eventlogger.executors",1);
		// conf.setNumWorkers(12);

		Properties p_ = new Properties();
		InputStream input = new FileInputStream(taskPropFilename);
		p_.load(input);

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout", new SampleSpout(argumentClass.getInputDatasetPathName(), spoutLogFileName,
				argumentClass.getScalingFactor()),
				1);

		String taskName = argumentClass.getTasksName();
		BaseTaskBolt taskBolt = MicroTopologyFactory.newTaskBolt(taskName, p_);
		builder.setBolt(taskName, taskBolt, 1).shuffleGrouping("spout");

		builder.setBolt("sink", new Sink(sinkLogFileName), 1).shuffleGrouping(taskName);

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


//	L   IdentityTopology   /Users/anshushukla/PycharmProjects/DataAnlytics1/Storm-Scheduler-SC-scripts/SYS-inputcsvSCTable-1spouts100mps-480sec.csv     PLUG-210  1.0   /Users/anshushukla/data/output/temp    /Users/anshushukla/Downloads/Incomplete/stream/iot-bm/modules/tasks/src/main/resources/tasks.properties    BlockWindowAverage
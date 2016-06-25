package in.dream_lab.bm.stream_iot.storm.topo.micro;

import in.dream_lab.bm.stream_iot.storm.bolts.AggregateBolts;
import in.dream_lab.bm.stream_iot.storm.bolts.BaseTaskBolt;

import java.util.Properties;

public class MicroTopologyFactory {

	public static BaseTaskBolt newTaskBolt(String taskName, Properties p) {
		switch(taskName) {
			//aggregate
			case "BlockWindowAverage" : return newBlockWindowAverageBolt(p); 
			case "DistinctApproxCount" : return newDistinctApproxCountBolt(p);
			//filter
			case "BloomFilterCheck" : return newBloomFilterCheckBolt(p);
			case "BloomFilterTrain" : return newBloomFilterTrainBolt(p);
			//io
			case "AzureBlobDownload" : return newAzureBlobDownloadTaskBolt(p);
			case "AzureBlobUpload" : return newAzureBlobUploadTaskBolt(p);
			case "AzureTable" : return newAzureTableTaskBolt(p);
			case "MQTTPublish" : return newMQTTPublishTaskBolt(p);
			//math
			case "PiByViete" : return newPiByVieteBolt(p);
			//parse
			case "XMLParse" : return newXMLParseBolt(p);
			//predict
			case "DecisionTreeClassify" : return newDecisionTreeClassifyBolt(p);
			case "DecisionTreeTrain" : return newDecisionTreeTrainBolt(p);
			case "LinearRegressionPredictor" : return newLinearRegressionPredictorBolt(p);
			case "LinearRegressionTrain" : return newLinearRegressionTrainBolt(p);
			case "SimpleLinearRegressionPredictor" : return newSimpleLinearRegressionPredictorBolt(p);
			//statistics
			case "KalmanFilter" : return newKalmanFilterBolt(p);
			case "SecondOrderMoment" : return newSecondOrderMomentBolt(p);


			default: throw new IllegalArgumentException("Unknown class name for bolt/task: " + taskName);
		}
		
	}

	public static BaseTaskBolt newBlockWindowAverageBolt(Properties p) {
		return new AggregateBolts.BlockWindowAverageBolt(p);
	}

	public static BaseTaskBolt newDistinctApproxCountBolt(Properties p) {
		return new AggregateBolts.DistinctApproxCountBolt(p);
	}

	//filter
	public static BaseTaskBolt newBloomFilterCheckBolt(Properties p) {
		return new AggregateBolts.BloomFilterCheckBolt(p);
	}
	public static BaseTaskBolt newBloomFilterTrainBolt(Properties p) {
		return new AggregateBolts.BloomFilterTrainBolt(p);
	}

	//io
	public static BaseTaskBolt newAzureBlobDownloadTaskBolt(Properties p) {
		return new AggregateBolts.AzureBlobDownloadTaskBolt(p);
	}
	public static BaseTaskBolt newAzureBlobUploadTaskBolt(Properties p) {
		return new AggregateBolts.AzureBlobUploadTaskBolt(p);
	}
	public static BaseTaskBolt newAzureTableTaskBolt(Properties p) {
		return new AggregateBolts.AzureTableTaskBolt(p);
	}
	public static BaseTaskBolt newMQTTPublishTaskBolt(Properties p) {
		return new AggregateBolts.MQTTPublishTaskBolt(p);
	}


	//math
	public static BaseTaskBolt newPiByVieteBolt(Properties p) {
		return new AggregateBolts.PiByVieteBolt(p);
	}

	//parse
	public static BaseTaskBolt newXMLParseBolt(Properties p) {
		return new AggregateBolts.XMLParseBolt(p);
	}

	//predict
	public static BaseTaskBolt newDecisionTreeClassifyBolt(Properties p) {
		return new AggregateBolts.DecisionTreeClassifyBolt(p);
	}
	public static BaseTaskBolt newDecisionTreeTrainBolt(Properties p) {
		return new AggregateBolts.DecisionTreeTrainBolt(p);
	}
	public static BaseTaskBolt newLinearRegressionPredictorBolt(Properties p) {
		return new AggregateBolts.LinearRegressionPredictorBolt(p);
	}
	public static BaseTaskBolt newLinearRegressionTrainBolt(Properties p) {
		return new AggregateBolts.LinearRegressionTrainBolt(p);
	}
	public static BaseTaskBolt newSimpleLinearRegressionPredictorBolt(Properties p) {
		return new AggregateBolts.SimpleLinearRegressionPredictorBolt(p);
	}

	//statistics
	public static BaseTaskBolt newKalmanFilterBolt(Properties p) {
		return new AggregateBolts.KalmanFilterBolt(p);
	}
	public static BaseTaskBolt newSecondOrderMomentBolt(Properties p) {
		return new AggregateBolts.SecondOrderMomentBolt(p);
	}


}


//	L   IdentityTopology   /Users/anshushukla/PycharmProjects/DataAnlytics1/Storm-Scheduler-SC-scripts/SYS-inputcsvSCTable-1spouts100mps-480sec.csv     PLUG-210  1.0   /Users/anshushukla/data/output/temp    /Users/anshushukla/Downloads/Incomplete/stream/iot-bm/modules/tasks/src/main/resources/tasks.properties    BlockWindowAverage
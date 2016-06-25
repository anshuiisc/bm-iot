package in.dream_lab.bm.stream_iot.storm.bolts.IoTPredictionBolts.SYS;

import in.dream_lab.bm.stream_iot.tasks.predict.LinearRegressionPredictor;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

public class LinearRegressionPredictorBolt extends BaseRichBolt {

    private Properties p;

    public LinearRegressionPredictorBolt(Properties p_){
         p=p_;
    }

    OutputCollector collector; private static Logger l;  public static void initLogger(Logger l_) {     l = l_; }
    LinearRegressionPredictor linearRegressionPredictor;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector=outputCollector; initLogger(LoggerFactory.getLogger("APP"));
        linearRegressionPredictor=new LinearRegressionPredictor();
        linearRegressionPredictor.setup(l,p);
    }

    @Override
    public void execute(Tuple input) {



        //


        String msgtype = input.getStringByField("msgtype");

        String obsVal="22.7,49.3,0,1955.22,27"; //dummy
        String msgId="0";
        String sensorMeta = "meta";

        if(msgtype.equals("modelupdate")){

            byte[] blobModelObjects = input.getBinaryByField("BlobModelObject");

            if(l.isInfoEnabled())
                l.info("blob model size "+blobModelObjects.length);

            // do nothing for now
        }

        else {
            obsVal = input.getStringByField("obsVal");
            msgId = input.getStringByField("MSGID");
            sensorMeta = input.getStringByField("sensorMeta");

            if(l.isInfoEnabled())
                l.info("obsVal-"+obsVal);
        }
        //



        Float res = linearRegressionPredictor.doTask(obsVal);

        l.info("res linearRegressionPredictor-"+res);

        if(res!=null ) {
            if(res!=Float.MIN_VALUE)
                collector.emit(new Values(sensorMeta,obsVal, msgId, res.toString(),"MLR"));
            else {
                if (l.isWarnEnabled()) l.warn("Error in LinearRegressionPredictorBolt");
                throw new RuntimeException();
            }
        }

    }

    @Override
    public void cleanup() {
        linearRegressionPredictor.tearDown();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sensorMeta","obsVal","MSGID","res","analyticsType"));
    }

}
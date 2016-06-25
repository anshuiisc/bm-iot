package in.dream_lab.bm.stream_iot.storm.bolts.IoTPredictionBolts.TAXI;

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

public class ErrorEstimationBolt extends BaseRichBolt {

    private Properties p;

    private String Res="0";
    private String avgRes="0";

    public ErrorEstimationBolt(Properties p_){
         p=p_;
    }
    OutputCollector collector; private static Logger l;  public static void initLogger(Logger l_) {     l = l_; }



    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {


        this.collector=outputCollector; initLogger(LoggerFactory.getLogger("APP"));

    }

// From L.R.
//    outputFieldsDeclarer.declare(new Fields("taxiMeta","obsVal","MSGID","Res","analyticsType"));

    @Override
    public void execute(Tuple input) {

        String msgId = input.getStringByField("MSGID");
        String analyticsType = input.getStringByField("analyticsType");
//        String sensorID=input.getStringByField("sensorID");
        String taxiMeta=input.getStringByField("taxiMeta");

        String obsVal = (input.getStringByField("obsVal"));



        if(analyticsType=="MLR") {
            Res  = input.getStringByField("res");
        }


        if(analyticsType=="AVG") {
            avgRes = input.getStringByField("avgRes");
        }


        float air_quality= Float.parseFloat(obsVal.split(",")[4]);

        if(l.isInfoEnabled())
            l.info("analyticsType:"+analyticsType+"-Res-"+Res+"-avgRes-"+avgRes);

        float errval= (air_quality-Float.parseFloat(Res)) /Float.parseFloat(avgRes);

        if(l.isInfoEnabled())
            l.info(("errval -"+errval));

//        if(avgRes!=null ) {

            collector.emit(new Values(taxiMeta, obsVal, msgId,analyticsType));

        }



    @Override
    public void cleanup() {
//       secondordermoment.tearDown();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("taxiMeta","res","MSGID","analyticsType"));
    }

}
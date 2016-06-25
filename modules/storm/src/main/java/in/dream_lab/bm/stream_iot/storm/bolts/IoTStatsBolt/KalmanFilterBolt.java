package in.dream_lab.bm.stream_iot.storm.bolts.IoTStatsBolt;

import in.dream_lab.bm.stream_iot.tasks.statistics.KalmanFilter;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KalmanFilterBolt extends BaseRichBolt {

    private Properties p;

    public KalmanFilterBolt(Properties p_){
         p=p_;
    }

    OutputCollector collector; private static Logger l;  public static void initLogger(Logger l_) {     l = l_; }
    Map<String, KalmanFilter> kmap; //kalmanFilter;
//    KalmanFilter kalmanFilter;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector=outputCollector; initLogger(LoggerFactory.getLogger("APP"));
        kmap = new HashMap<String, KalmanFilter>();

//        kalmanFilter=new KalmanFilter();
//        kalmanFilter.setup(l,p);
    }

//from bloom -    outputFieldsDeclarer.declare(new Fields("sensorMeta","sensorID","obsType","obsVal","MSGID"));

    @Override
    public void execute(Tuple input) {

        String msgId = input.getStringByField("MSGID");
        String sensorMeta=input.getStringByField("sensorMeta");
        String sensorID=input.getStringByField("sensorID");
        String obsType=input.getStringByField("obsType");
        String obsVal = input.getStringByField("obsVal");

        String key = sensorID + obsType;


        KalmanFilter kalmanFilter = kmap.get(key);
        if(kalmanFilter == null){
            kalmanFilter=new KalmanFilter();
            kalmanFilter.setup(l,p);
            kmap.put(key, kalmanFilter);
        }



        Float kalmanUpdatedVal =kalmanFilter.doTask(obsVal);

        if(l.isInfoEnabled())
        l.info("TEST1:kalmanUpdatedVal-"+kalmanUpdatedVal);

        if(kalmanUpdatedVal!=null ) {
            collector.emit(new Values(sensorMeta, sensorID, obsType, kalmanUpdatedVal.toString(), msgId));
        }
            else {
                if (l.isWarnEnabled()) l.warn("Error in KalmanFilterBolt and Val is -"+kalmanUpdatedVal);
                throw new RuntimeException();
            }
    }

    @Override
    public void cleanup() {
//        kalmanFilter.tearDown();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sensorMeta","sensorID","obsType","kalmanUpdatedVal","MSGID"));
    }

}
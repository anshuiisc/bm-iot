package in.dream_lab.bm.stream_iot.storm.bolts.IoTStatsBolt;

import in.dream_lab.bm.stream_iot.tasks.aggregate.DistinctApproxCount;
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

public class DistinctApproxCountBolt extends BaseRichBolt {

    private Properties p;

    public DistinctApproxCountBolt(Properties p_){
         p=p_;
    }
    OutputCollector collector; private static Logger l;  public static void initLogger(Logger l_) {     l = l_; }
    DistinctApproxCount distinctApproxCount;
//    Map<String, DistinctApproxCount> distinctApproxCountMap ;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector=outputCollector; initLogger(LoggerFactory.getLogger("APP"));

//        distinctApproxCountMap = new HashMap<String, DistinctApproxCount>();

        distinctApproxCount=new DistinctApproxCount();
        distinctApproxCount.setup(l,p);
    }

    @Override
    public void execute(Tuple input) {

        String msgId = input.getStringByField("MSGID");
        String sensorMeta=input.getStringByField("sensorMeta");
        String sensorID=input.getStringByField("sensorID");

        String obsType=input.getStringByField("obsType");

        distinctApproxCount.doTask(sensorID);
        Float res= (Float) distinctApproxCount.getLastResult();

        if(l.isInfoEnabled())
            l.info("distinctApproxCount:"+res);

        if(res!=null ) {
            if(res!=Float.MIN_VALUE)
                collector.emit(new Values(sensorMeta,sensorID,obsType,res.toString(),msgId));
            else {
                if (l.isWarnEnabled()) l.warn("Error in SecondOrderMomentBolt");
                throw new RuntimeException();
            }
        }
    }

    @Override
    public void cleanup()
    {
//        distinctApproxCount.tearDown();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sensorMeta","sensorID","obsType","res","MSGID"));
    }

}
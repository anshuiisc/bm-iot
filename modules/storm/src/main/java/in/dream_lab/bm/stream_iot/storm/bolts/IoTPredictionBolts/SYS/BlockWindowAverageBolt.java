package in.dream_lab.bm.stream_iot.storm.bolts.IoTPredictionBolts.SYS;

import in.dream_lab.bm.stream_iot.tasks.aggregate.BlockWindowAverage;
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

public class BlockWindowAverageBolt extends BaseRichBolt {

    private Properties p;

    public BlockWindowAverageBolt(Properties p_){
         p=p_;
    }
    OutputCollector collector; private static Logger l;  public static void initLogger(Logger l_) {     l = l_; }
    BlockWindowAverage blockWindowAverage;
//    Map<String, BlockWindowAverage> blockWindowAverageMap;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector=outputCollector; initLogger(LoggerFactory.getLogger("APP"));

//        blockWindowAverageMap = new HashMap<String, BlockWindowAverage>();

        blockWindowAverage=new BlockWindowAverage();
        blockWindowAverage.setup(l,p);
    }

//    outputFieldsDeclarer.declare(new Fields("sensorMeta","sensorID","obsVal","MSGID"));

    @Override
    public void execute(Tuple input) {

        String sensorMeta=input.getStringByField("sensorMeta");
        String sensorID=input.getStringByField("sensorID");
        String obsVal = input.getStringByField("obsVal");
        String msgId = input.getStringByField("MSGID");

//
//        String key = sensorID + obsType;
//        BlockWindowAverage blockWindowAverage = blockWindowAverageMap.get(key);
//        if(blockWindowAverage == null){
//            blockWindowAverage=new BlockWindowAverage();
//            blockWindowAverage.setup(l,p);
//            blockWindowAverageMap.put(key, blockWindowAverage);
//        }



        Float avgRes = blockWindowAverage.doTask(obsVal.split(",")[1]);


        if(l.isInfoEnabled())
            l.info("blockWindowAverage:"+avgRes);


        if(avgRes!=null ) {
            if(avgRes!=Float.MIN_VALUE) {

                collector.emit(new Values(sensorMeta,obsVal,avgRes.toString(),msgId,"AVG"));
            }
            else {
                if (l.isWarnEnabled()) l.warn("Error in BlockWindowAverageBolt");
                throw new RuntimeException();
            }
        }
    }

    @Override
    public void cleanup() {
//       secondordermoment.tearDown();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sensorMeta","obsVal","avgRes","MSGID","analyticsType"));
    }

}
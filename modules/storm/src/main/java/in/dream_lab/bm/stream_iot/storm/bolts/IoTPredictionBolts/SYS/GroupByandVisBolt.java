package in.dream_lab.bm.stream_iot.storm.bolts.IoTPredictionBolts.SYS;

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

public class GroupByandVisBolt extends BaseRichBolt {

    private Properties p;
    private int wind=0;
    StringBuilder sb;

    public GroupByandVisBolt(Properties p_){
         p=p_;
    }
    OutputCollector collector; private static Logger l;  public static void initLogger(Logger l_) {     l = l_; }

    Map<String, StringBuilder> dataMap; //kalmanFilter;



    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector=outputCollector; initLogger(LoggerFactory.getLogger("APP"));
//        sb=new StringBuilder();


        dataMap = new HashMap<String, StringBuilder>();

    }

//    from error
//    outputFieldsDeclarer.declare(new Fields("sensorMeta","sensorID","res","MSGID","analyticsType"));

//    from DTC
//    outputFieldsDeclarer.declare(new Fields("obsVal","MSGID","res","analyticsType"));

    @Override
    public void execute(Tuple input) {

        String msgId = input.getStringByField("MSGID");
        String analyticsType = input.getStringByField("analyticsType");
//        String sensorMeta=input.getStringByField("sensorMeta");
        //        String sensorID=input.getStringByField("sensorID");
//        String obsVal = (input.getStringByField("obsVal"));

        String Res=input.getStringByField("res");

//        float avgRes=Float.parseFloat(input.getStringByField("avgRes"));


        String key =  analyticsType;
        StringBuilder sb = dataMap.get(key);
        if(sb == null){
            sb=new StringBuilder();
            dataMap.put(key, sb);
        }


        if(wind<60)
        {
            sb.append(Res+analyticsType);
            wind+=1;

        }
        else {
            if(l.isInfoEnabled())
                l.info("concatRes-"+sb.toString());

            collector.emit(new Values(sb.toString() , msgId));
            sb.setLength(0);
            wind=0;
        }


        }



    @Override
    public void cleanup() {
//       secondordermoment.tearDown();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("res","MSGID"));
    }

}
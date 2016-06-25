package in.dream_lab.bm.stream_iot.storm.bolts.IoTPredictionBolts.TAXI;

import in.dream_lab.bm.stream_iot.tasks.predict.DecisionTreeClassify;
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

public class DecisionTreeClassifyBolt extends BaseRichBolt {

    private Properties p;

    public DecisionTreeClassifyBolt(Properties p_){
         p=p_;

    }
    OutputCollector collector; private static Logger l;  public static void initLogger(Logger l_) {     l = l_; }

    DecisionTreeClassify decisionTreeClassifyBolt;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector=outputCollector;
        initLogger(LoggerFactory.getLogger("APP"));

        decisionTreeClassifyBolt=new DecisionTreeClassify();

        decisionTreeClassifyBolt.setup(l,p);
    }

    @Override
    public void execute(Tuple input) {

        String msgtype = input.getStringByField("msgtype");

        String obsVal="22.7,49.3,0,1955.22,27"; //dummy
        String msgId="0";

        if(msgtype=="modelupdate"){
            // do nothing for now
        }

        else {
             obsVal = input.getStringByField("obsVal");
             msgId = input.getStringByField("MSGID");
        }






        Float res = decisionTreeClassifyBolt.doTask(obsVal);  // index of result-class/enum as return

        if(res!=null ) {
            if(res!=Float.MIN_VALUE)
                collector.emit(new Values(obsVal, msgId, res.toString(),"DTC"));
            else {
                if (l.isWarnEnabled()) l.warn("Error in DecisionTreeClassifyBolt");
                throw new RuntimeException();
            }
        }
    }

    @Override
    public void cleanup() {
        decisionTreeClassifyBolt.tearDown();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("obsVal","MSGID","res","analyticsType"));
    }

}
package in.dream_lab.bm.stream_iot.storm.bolts.IoTPredictionBolts.TAXI;

import in.dream_lab.bm.stream_iot.tasks.io.AzureTableTask;
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

public class AzureTableTaskBolt extends BaseRichBolt {

    private Properties p;


    public AzureTableTaskBolt(Properties p_){
         p=p_;
    }
    OutputCollector collector; private static Logger l;  public static void initLogger(Logger l_) {     l = l_; }

    AzureTableTask azureTableTask;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector=outputCollector; initLogger(LoggerFactory.getLogger("APP"));

        azureTableTask=new AzureTableTask();
        azureTableTask.setup(l,p);
    }

    @Override
    public void execute(Tuple input) {
        String rowString = input.getStringByField("RowString");
        String msgId = input.getStringByField("MSGID");

        Float res = azureTableTask.doTask(rowString);

//    TODO:for AS    if(res!=-1)

        if(res!=null ) {
            if(res!=Float.MIN_VALUE)
                collector.emit(new Values(rowString, msgId, res));
            else {
                if (l.isWarnEnabled()) l.warn("Error in AzureTableTaskBolt");
                throw new RuntimeException();
            }
        }
    }

    @Override
    public void cleanup() {
        azureTableTask.tearDown();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("RowString","MSGID","res"));
    }

}
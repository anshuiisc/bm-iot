package in.dream_lab.bm.stream_iot.storm.spouts;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;


public class TimeSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;

    static long timerWindowinMilliSec=86;

    long startTimer=0;
    long currentTime;


    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        _collector= spoutOutputCollector;
        startTimer=System.currentTimeMillis();
    }

    public void nextTuple() {
        // TODO Read packet and forward to next bolt

        currentTime=System.currentTimeMillis();

        if((currentTime-startTimer)>timerWindowinMilliSec)
        {
            _collector.emit(new Values(Long.toString(currentTime)));
            startTimer=currentTime;
        }



    }


    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("RowString"));
    }
}
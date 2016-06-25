package in.dream_lab.bm.stream_iot.storm.bolts.IoTPredictionBolts.SYS;

import in.dream_lab.bm.stream_iot.tasks.io.AzureBlobUploadTask;
import org.apache.storm.shade.org.apache.commons.io.FileUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class AzureBlobUploadTaskBolt extends BaseRichBolt {

    private Properties p;

    public AzureBlobUploadTaskBolt(Properties p_){
         p=p_;

    }

    OutputCollector collector;
    private static Logger l; // TODO: Ensure logger is initialized before use
    public static void initLogger(Logger l_) {
        l = l_;
    }

    AzureBlobUploadTask azureBlobUploadTask;

    String fileName="T";

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector=outputCollector;
        initLogger(LoggerFactory.getLogger("APP"));

        azureBlobUploadTask=new AzureBlobUploadTask();

        //ToDO:  unique file path for every thread in local before uploading


        String baseDirname=p.getProperty("IO.AZURE_BLOB_UPLOAD.DIR_NAME").toString();
        fileName= baseDirname+"/"+UUID.randomUUID().toString();


        p.setProperty("IO.AZURE_BLOB_UPLOAD.FILE_SOURCE_PATH",fileName);
        azureBlobUploadTask.setup(l,p);


    }

    @Override
    public void execute(Tuple input) {
        String res = input.getStringByField("res");
        String msgId = input.getStringByField("MSGID");

        try {
            FileUtils.writeStringToFile(new File(fileName), res);
        } catch (IOException e) {
            e.printStackTrace();
        }

        Float blobRes = azureBlobUploadTask.doTask("1");

// TODO: previous check      if(res==1)

        if(res!=null ) {
            if(blobRes!=Float.MIN_VALUE)
                collector.emit(new Values(msgId));
            else {
                if (l.isWarnEnabled()) l.warn("Error in AzureBlobUploadTaskBolt");
                throw new RuntimeException();
            }
        }
    }

    @Override
    public void cleanup() {
        azureBlobUploadTask.tearDown();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("MSGID"));
    }

}
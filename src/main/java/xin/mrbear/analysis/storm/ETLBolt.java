package xin.mrbear.analysis.storm;


import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import xin.mrbear.analysis.storm.pojo.Message;

/**
 * 做Etl的工作
 * --------------------------------正常情况下这里的代码量很多
 */
public class ETLBolt extends BaseBasicBolt {

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String data = tuple.getStringByField("data");
        String[] fields = data.split("\t");
        if(fields.length == 13){
            Message message = new Message(fields);
            basicOutputCollector.emit(new Values(message));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("message"));
    }
}

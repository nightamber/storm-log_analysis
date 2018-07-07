package xin.mrbear.analysis.storm;


import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import xin.mrbear.analysis.storm.pojo.Message;
import xin.mrbear.analysis.storm.util.LogAnalyerHandler;

public class ProcessBolt extends BaseBasicBolt {

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        // 1. 获取上游发送的数据 得到一个message对象
        // 2.从数据库中加载任务信息，开始匹配并计算
        // 3.将匹配到的任务结果保存到redis中
        Message message = (Message) tuple.getValue(0);
        LogAnalyerHandler.process(message);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}

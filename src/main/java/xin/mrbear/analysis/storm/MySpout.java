package xin.mrbear.analysis.storm;

import java.util.Map;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * 这是一个模拟类。用来生成log日志的。
 * 正常情况下，应该是使用kafkaSpout去kafka集群中消费数据。
 */
public class MySpout extends BaseRichSpout {
    private SpoutOutputCollector spoutOutputCollector;

    public void open(Map map, TopologyContext topologyContext,
        SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
    }

    public void nextTuple() {
        // todo 这里面会发送一条数据 这条数据是伪造的
        String data = "http://www.itcast.cn/\thttp://www.itcast.cn/product?id=1002\t111\t1\t30\t192.168.114.123\tsid12345\tzhangsan\th|keycount|head|category_02a\twin\tchrome\t1366*768\t123*345";
        spoutOutputCollector.emit(new Values(data));
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("data"));
    }
}

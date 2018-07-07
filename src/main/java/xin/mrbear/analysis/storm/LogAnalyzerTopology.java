package xin.mrbear.analysis.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class LogAnalyzerTopology {

    public static void main(String[] args) {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("kafkaSpout",new MySpout());
        topologyBuilder.setBolt("etlBolt",new ETLBolt()).localOrShuffleGrouping("kafkaSpout");
        topologyBuilder.setBolt("processBolt",new ProcessBolt()).localOrShuffleGrouping("etlBolt");

        Config config = new Config();
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("loganalyzer",config,topologyBuilder.createTopology());
    }
}

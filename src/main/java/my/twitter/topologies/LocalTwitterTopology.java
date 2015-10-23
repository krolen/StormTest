package my.twitter.topologies;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import my.twitter.bolts.ErrorBolt;
import my.twitter.bolts.ParserBolt;
import my.twitter.bolts.ProfileLogBolt;
import my.twitter.bolts.TwitterLogBolt;

/**
 * Created by kkulagin on 10/23/2015.
 */
public class LocalTwitterTopology {
  public static void main(String[] args) {
    Config conf = new Config();
    conf.setDebug(true);
    conf.setNumWorkers(2);

    LocalCluster cluster = new LocalCluster();
//    cluster.submitTopology("test", conf, builder.createTopology());
    Utils.sleep(10000);
    cluster.killTopology("test");
    cluster.shutdown();
  }

  private StormTopology createTopology() {
    TopologyBuilder builder = new TopologyBuilder();
//    builder.setSpout("kafkaSpout", createKafkaSpout(), 1);
    builder.setBolt("parserBolt", new ParserBolt(), 6).setNumTasks(6).shuffleGrouping("kafkaSpout");
    builder.setBolt("profileBolt", new ProfileLogBolt(), 6).setNumTasks(6).shuffleGrouping("parserBolt", "profile");
    builder.setBolt("tweetsBolt", new TwitterLogBolt(), 6).setNumTasks(6).shuffleGrouping("parserBolt");
    builder.setBolt("errorBolt", new ErrorBolt(), 3).setNumTasks(3).shuffleGrouping("parserBolt", "err");
    return builder.createTopology();
  }

}

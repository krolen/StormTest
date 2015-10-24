package my.twitter.topologies;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import my.twitter.bolts.ErrorBolt;
import my.twitter.bolts.ParserBolt;
import my.twitter.bolts.ProfileLogBolt;
import my.twitter.bolts.TweetLogBolt;
import my.twitter.spout.SampleTwitterSpout;

import java.util.concurrent.TimeUnit;

/**
 * Created by kkulagin on 10/23/2015.
 */
public class LocalTwitterTopology {
  public static void main(String[] args) {

    String topologyName = "sampleTwitterStream";

    Config conf = new Config();
    conf.setDebug(true);
    conf.setNumWorkers(2);

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology(topologyName, conf, createTopology());
    Uninterruptibles.sleepUninterruptibly(20, TimeUnit.SECONDS);
    cluster.killTopology(topologyName);
    cluster.shutdown();
  }

  private static StormTopology createTopology() {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("sampleTweetsSpout", new SampleTwitterSpout(), 1);
    builder.setBolt("parserBolt", new ParserBolt(), 6).setNumTasks(6).shuffleGrouping("sampleTweetsSpout");
    builder.setBolt("profileBolt", new ProfileLogBolt(), 6).setNumTasks(6).shuffleGrouping("parserBolt", "profile");
    builder.setBolt("tweetsBolt", new TweetLogBolt(), 6).setNumTasks(6).shuffleGrouping("parserBolt", "tweet");
    builder.setBolt("errorBolt", new ErrorBolt(), 3).setNumTasks(3).shuffleGrouping("parserBolt", "err");
    return builder.createTopology();
  }

}

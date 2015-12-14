package my.twitter.topologies;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import my.twitter.beans.DeleteTweet;
import my.twitter.beans.Profile;
import my.twitter.beans.Tweet;
import my.twitter.bolts.ErrorBolt;
import my.twitter.bolts.ParserBolt;
import my.twitter.bolts.profile.ProfileLogBolt;
import my.twitter.bolts.tweet.TweetLogBolt;
import my.twitter.spout.TwitterSchema;
import my.twitter.utils.LogAware;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

/**
 * Created by kkulagin on 5/13/2015.
 */
public class KafkaTweetsTopology extends TwitterTopology {

  @Override
  protected IRichSpout createRootSpout() {
    BrokerHosts zkhosts = new ZkHosts("PUT DATA HERE");
    String topic = "tweets";
    String zkRoot = "";
    String consumerGroupId = "storm-test";
    SpoutConfig spoutConfig = new SpoutConfig(zkhosts, topic, zkRoot, consumerGroupId);
    spoutConfig.scheme = new SchemeAsMultiScheme(new TwitterSchema());
    return new KafkaSpout(spoutConfig);
  }

  @Override
  protected Config config() {
    Config conf = super.config();
    conf.put(Config.NIMBUS_HOST, "52.8.44.60");
    conf.put(Config.NIMBUS_THRIFT_PORT, 6627);
    conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 6);
    return conf;
  }

  public static void main(String[] args) {
    System.setProperty("storm.jar", "StormTest.jar");

    KafkaTweetsTopology thisTopology = new KafkaTweetsTopology();
    try {
      StormSubmitter.submitTopology("myTestTopology", thisTopology.config(), thisTopology.topology());
    } catch (Exception e) {
      e.printStackTrace();
      System.err.println("Error submitting Topology" +  e.getMessage());
    }
  }
}

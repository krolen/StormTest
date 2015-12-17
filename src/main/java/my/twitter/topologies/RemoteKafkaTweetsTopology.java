package my.twitter.topologies;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.IRichSpout;
import my.twitter.spout.TwitterSchema;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

/**
 * Created by kkulagin on 5/13/2015.
 */
public class RemoteKafkaTweetsTopology extends TwitterTopology {

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
    return conf;
  }

  public static void main(String[] args) {
    System.setProperty("storm.jar", "StormTest.jar");

    RemoteKafkaTweetsTopology thisTopology = new RemoteKafkaTweetsTopology();
    try {
      StormSubmitter.submitTopology("myTestTopology", thisTopology.config(), thisTopology.topology());
    } catch (Exception e) {
      e.printStackTrace();
      System.err.println("Error submitting Topology" +  e.getMessage());
    }
  }
}

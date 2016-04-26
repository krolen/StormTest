package my.twister.storm.topologies;

import my.twister.storm.spout.TwitterSchema;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.IRichSpout;

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

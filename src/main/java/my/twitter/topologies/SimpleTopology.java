package my.twitter.topologies;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import my.twitter.bolts.ErrorBolt;
import my.twitter.bolts.ParserBolt;
import my.twitter.bolts.ProfileLogBolt;
import my.twitter.bolts.TwitterLogBolt;
import my.twitter.spout.TwitterSchema;
import org.slf4j.LoggerFactory;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

/**
 * Created by kkulagin on 5/13/2015.
 */
public class SimpleTopology {

  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(TwitterLogBolt.class);

  public SimpleTopology() {
    logger.error("SimpleTopology constructor");
    System.out.println("SimpleTopology constructor");
  }


  private IRichSpout createKafkaSpout() {
    BrokerHosts zkhosts = new ZkHosts("54.193.49.78:2181,54.193.49.64:2181,54.193.51.90:2181,54.193.42.254:2181");
    //    BrokerHosts zkhosts = new ZkHosts("PUT DATA HERE");
    String topic = "tweets";
    String zkRoot = "";
    String consumerGroupId = "storm-test";
    SpoutConfig spoutConfig = new SpoutConfig(zkhosts, topic, zkRoot, consumerGroupId);
    spoutConfig.scheme = new SchemeAsMultiScheme(new TwitterSchema());
    KafkaSpout kafkaspout = new KafkaSpout(spoutConfig);
    return kafkaspout;
  }


  private IRichBolt createLoggerBolt() {
    TwitterLogBolt bolt = new TwitterLogBolt();
    return bolt;
  }


  private StormTopology createTopology() {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("kafkaSpout", createKafkaSpout(), 1);
    builder.setBolt("parserBolt", new ParserBolt(), 6).setNumTasks(6).shuffleGrouping("kafkaSpout");
    builder.setBolt("profileBolt", new ProfileLogBolt(), 6).setNumTasks(6).shuffleGrouping("parserBolt", "profile");
    builder.setBolt("tweetsBolt", new TwitterLogBolt(), 6).setNumTasks(6).shuffleGrouping("parserBolt");
    builder.setBolt("errorBolt", new ErrorBolt(), 3).setNumTasks(3).shuffleGrouping("parserBolt", "err");
    return builder.createTopology();
  }

  public static void main(String[] args) {
    System.setProperty("storm.jar", "StormTest.jar");


    SimpleTopology topology = new SimpleTopology();
    Config conf = new Config();
//    conf.setDebug(true);
    conf.put(Config.TOPOLOGY_WORKERS, 1);
    conf.put(Config.NIMBUS_HOST, "52.8.44.60");
    conf.put(Config.NIMBUS_THRIFT_PORT, 6627);
    conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 6);


    try {
      StormSubmitter.submitTopology("myTestTopology", conf, topology.createTopology());
    } catch (Exception e) {
      logger.error("Error submitting Topology", e);
    }

  }
}

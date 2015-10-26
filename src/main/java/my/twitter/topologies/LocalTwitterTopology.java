package my.twitter.topologies;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import my.twitter.beans.DeleteTweet;
import my.twitter.beans.Profile;
import my.twitter.beans.Tweet;
import my.twitter.bolts.*;
import my.twitter.bolts.profile.ProfileLogBolt;
import my.twitter.bolts.profile.chronicle.Name2IdBolt;
import my.twitter.bolts.tweet.DeleteTweetLogBolt;
import my.twitter.bolts.tweet.TweetLogBolt;
import my.twitter.spout.SampleTwitterSpout;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Created by kkulagin on 10/23/2015.
 */
public class LocalTwitterTopology {

  public static void main(String[] args) throws IOException {

    String topologyName = "sampleTwitterStream";

    Config conf = new Config();
    conf.setDebug(true);
    conf.setNumWorkers(2);
    conf.registerSerialization(Tweet.class);
    conf.registerSerialization(Profile.class);
    conf.registerSerialization(DeleteTweet.class);

    configureChronicleMapProperties(conf);

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology(topologyName, conf, createTopology());
    Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
    cluster.killTopology(topologyName);
    cluster.shutdown();
  }

  private static StormTopology createTopology() {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("sampleTweetsSpout", new SampleTwitterSpout(), 1);
    builder.setBolt("parserBolt", new ParserBolt(), 2).setNumTasks(2).shuffleGrouping("sampleTweetsSpout");
    builder.setBolt("deleteTweetLogBolt", new DeleteTweetLogBolt(), 2).setNumTasks(2).shuffleGrouping("parserBolt", "deleteTweet");
    builder.setBolt("name2IdBolt", new Name2IdBolt(), 2).setNumTasks(2).shuffleGrouping("parserBolt", "profile");
//    builder.setBolt("profileBolt", new ProfileLogBolt(), 2).setNumTasks(2).shuffleGrouping("parserBolt", "profile");
    builder.setBolt("tweetsBolt", new TweetLogBolt(), 2).setNumTasks(2).shuffleGrouping("parserBolt", "tweet");
    builder.setBolt("errorBolt", new ErrorBolt(), 3).setNumTasks(3).shuffleGrouping("parserBolt", "err");
    return builder.createTopology();
  }

  private static void configureChronicleMapProperties(Config config) throws IOException {
    final Properties properties = new Properties();
    try (InputStream stream = LocalTwitterTopology.class.getResourceAsStream("/hft.properties")) {
      properties.load(stream);
    }
    propagateRequiredValue(config, properties, "profile.name.to.id.file");
    propagateRequiredValue(config, properties, "profile.id.to.profile.file");
  }

  private static void propagateRequiredValue(Config config, Properties properties, String propName) {
    String propValue = Optional.ofNullable(properties.getProperty(propName)).orElseThrow(() -> new RuntimeException("Property " + propName + " was not found"));
    config.put(propName, propValue);
  }

}

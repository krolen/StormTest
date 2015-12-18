package my.twitter.topologies;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import my.twitter.beans.DeleteTweet;
import my.twitter.beans.Profile;
import my.twitter.beans.Tweet;
import my.twitter.bolts.ErrorBolt;
import my.twitter.bolts.ParserBolt;
import my.twitter.bolts.profile.StoreProfileBolt;
import my.twitter.bolts.profile.chronicle.AmendProfileBolt;
import my.twitter.bolts.tweet.DeleteTweetLogBolt;
import my.twitter.bolts.tweet.TweetLogBolt;
import my.twitter.utils.LogAware;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.Properties;

/**
 * Created by kkulagin on 12/14/2015.
 */
public abstract class TwitterTopology implements LogAware {

  protected void configureChronicleMapProperties(Config config) {
    final Properties properties = new Properties();
    try (InputStream stream = SampleTwitterTopology.class.getResourceAsStream("/hft.properties")) {
      properties.load(stream);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    propagateRequiredValue(config, properties, "profile.name.to.id.file");
    propagateRequiredValue(config, properties, "profile.id.to.profile.file");
  }

  private static void propagateRequiredValue(Config config, Properties properties, String propName) {
    String propValue = Optional.ofNullable(properties.getProperty(propName)).orElseThrow(() -> new RuntimeException("Property " + propName + " was not found"));
    config.put(propName, propValue);
  }

  protected abstract IRichSpout createRootSpout();

  protected Config config() {
    Config conf = new Config();
    conf.setDebug(true);
    conf.setNumWorkers(1);
    conf.registerSerialization(Tweet.class);
    conf.registerSerialization(Profile.class);
    conf.registerSerialization(DeleteTweet.class);

    conf.put(Config.NIMBUS_THRIFT_PORT, 6627);
    conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 6);

    configureChronicleMapProperties(conf);
    return conf;
  }

  protected StormTopology topology() {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("rootSpout", createRootSpout(), 1);
    builder.setBolt("parserBolt", new ParserBolt(), 2).setNumTasks(2).shuffleGrouping("rootSpout");

    builder.setBolt("deleteTweetLogBolt", new DeleteTweetLogBolt(), 2).setNumTasks(2).shuffleGrouping("parserBolt", "deleteTweet");

    builder.setBolt("amendProfileBolt", new AmendProfileBolt(), 2).setNumTasks(2).shuffleGrouping("parserBolt", "profile");
    builder.setBolt("storeProfileBolt", new StoreProfileBolt(), 1).setNumTasks(1).shuffleGrouping("amendProfileBolt", "storeProfile");

//    builder.setBolt("profileBolt", new ProfileLogBolt(), 2).setNumTasks(2).shuffleGrouping("parserBolt", "profile");
    builder.setBolt("tweetsBolt", new TweetLogBolt(), 2).setNumTasks(2).shuffleGrouping("parserBolt", "tweet");
    builder.setBolt("errorBolt", new ErrorBolt(), 3).setNumTasks(3).shuffleGrouping("parserBolt", "err");
    return builder.createTopology();
  }
}

package my.twister.storm.topologies;

import my.twister.storm.beans.DeleteTweet;
import my.twister.storm.beans.Profile;
import my.twister.storm.beans.Tweet;
import my.twister.storm.bolts.ErrorBolt;
import my.twister.storm.bolts.ParserBolt;
import my.twister.storm.bolts.profile.AmendProfileBolt;
import my.twister.storm.bolts.profile.StoreProfileBolt;
import my.twister.storm.bolts.stuff.AnomalyLogBolt;
import my.twister.storm.bolts.tweet.DeleteTweetLogBolt;
import my.twister.storm.bolts.tweet.StoreTweetBolt;
import my.twister.storm.bolts.tweet.TweetIndexerBolt;
import my.twister.storm.bolts.tweet.TweetMentionsBolt;
import my.twister.utils.LogAware;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.metric.LoggingMetricsConsumer;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Created by kkulagin on 12/14/2015.
 */
public abstract class TwitterTopology implements LogAware {

  protected abstract IRichSpout createRootSpout();

  protected Config config() {
    Config conf = new Config();
    conf.setDebug(true);
    conf.setNumWorkers(1);
    conf.registerSerialization(Tweet.class);
    conf.registerSerialization(Profile.class);
    conf.registerSerialization(DeleteTweet.class);

    conf.registerMetricsConsumer(LoggingMetricsConsumer.class, 1);

    conf.put(Config.TOPOLOGY_DEBUG, false);
    conf.put(Config.NIMBUS_THRIFT_PORT, 6627);
    conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 6);

    return conf;
  }

  protected StormTopology topology() {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("rootSpout", createRootSpout(), 1);
    builder.setBolt("parserBolt", new ParserBolt(), 3).setNumTasks(3).shuffleGrouping("rootSpout");

    builder.setBolt("anomalyBolt", new AnomalyLogBolt(), 1).setNumTasks(1).shuffleGrouping("parserBolt", "anomaly");

    builder.setBolt("deleteTweetLogBolt", new DeleteTweetLogBolt(), 1).setNumTasks(1).shuffleGrouping("parserBolt", "deleteTweet");

    builder.setBolt("amendProfileBolt", new AmendProfileBolt(), 3).setNumTasks(2).shuffleGrouping("parserBolt", "profile");

    builder.setBolt("storeProfileBolt", new StoreProfileBolt(), 1).setNumTasks(1).shuffleGrouping("amendProfileBolt", "storeProfile");

    builder.setBolt("tweetMentions", new TweetMentionsBolt(), 2).setNumTasks(2).shuffleGrouping("parserBolt", "tweet");

    builder.setBolt("storeTweetBolt", new StoreTweetBolt(), 1).setNumTasks(1).shuffleGrouping("tweetMentions", "tweet");

    builder.setBolt("tweetIndexer", new TweetIndexerBolt(), 2).setNumTasks(2).shuffleGrouping("parserBolt", "tweet");

    builder.setBolt("errorBolt", new ErrorBolt(), 1).setNumTasks(1).shuffleGrouping("parserBolt", "err");

    StormTopology topology = builder.createTopology();
    return topology;
  }
}

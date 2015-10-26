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
    conf.registerSerialization(Tweet.class);
    conf.registerSerialization(Profile.class);
    conf.registerSerialization(DeleteTweet.class);

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
    builder.setBolt("profileBolt", new ProfileLogBolt(), 2).setNumTasks(2).shuffleGrouping("parserBolt", "profile");
    builder.setBolt("tweetsBolt", new TweetLogBolt(), 2).setNumTasks(2).shuffleGrouping("parserBolt", "tweet");
    builder.setBolt("errorBolt", new ErrorBolt(), 3).setNumTasks(3).shuffleGrouping("parserBolt", "err");
    return builder.createTopology();
  }

}

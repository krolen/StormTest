package my.twister.storm.topologies;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import my.twister.storm.beans.Profile;
import my.twister.storm.beans.Tweet;
import my.twister.storm.bolts.ErrorBolt;
import my.twister.storm.bolts.ParserBolt;
import my.twister.storm.bolts.profile.AmendProfileBolt;
import my.twister.storm.bolts.profile.StoreProfileBolt;
import my.twister.storm.bolts.stuff.AnomalyLogBolt;
import my.twister.storm.bolts.stuff.LogBolt;
import my.twister.storm.bolts.tweet.DeleteTweetLogBolt;
import my.twister.storm.bolts.tweet.StoreTweetBolt;
import my.twister.storm.bolts.tweet.TweetIndexerBolt;
import my.twister.storm.bolts.tweet.TweetMentionsBolt;
import my.twister.storm.spout.FileTestSpout;
import my.twister.utils.Constants;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Created by kkulagin on 10/23/2015.
 */
public class FileTwitterTopology extends TwitterTopology {


  @Override
  protected IRichSpout createRootSpout() {
    return new FileTestSpout();
  }

  @Override
  protected Config config() {
    Config conf = super.config();
    conf.put(FileTestSpout.TWEETS_FILE_LOCATION, "C:\\Projects\\Twister\\StormTest\\target\\classes\\realSampleTweet2.json");
    conf.put(FileTestSpout.RATE_LIMIT, 5000);
    return conf;
  }

  @Override
  protected StormTopology topology() {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("rootSpout", createRootSpout(), 1);
    builder.setBolt("parserBolt", new ParserBolt(), 3).setNumTasks(2).shuffleGrouping("rootSpout");

    builder.setBolt("logProfileBolt", new LogBolt<Profile>(), 3).setNumTasks(2).shuffleGrouping("parserBolt", "profile");

    builder.setBolt("logTweetBolt", new LogBolt<Tweet>(), 1).setNumTasks(1).shuffleGrouping("parserBolt", "tweet");

    builder.setBolt("errorBolt", new ErrorBolt(), 1).setNumTasks(1).shuffleGrouping("parserBolt", "err");

    StormTopology topology = builder.createTopology();
    return topology;
  }

  public static void main(String[] args) throws IOException {

    LocalCluster cluster = new LocalCluster();

    TwitterTopology thisTopology = new FileTwitterTopology();
    String topologyName = "sampleTwitterStream";
    cluster.submitTopology(topologyName, thisTopology.config(), thisTopology.topology());
    Uninterruptibles.sleepUninterruptibly(200, TimeUnit.SECONDS);
    cluster.killTopology(topologyName);
    cluster.shutdown();
  }

}

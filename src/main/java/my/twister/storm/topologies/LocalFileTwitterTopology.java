package my.twister.storm.topologies;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;
import my.twister.storm.beans.Profile;
import my.twister.storm.beans.Tweet;
import my.twister.storm.bolts.ErrorBolt;
import my.twister.storm.bolts.ParserBolt;
import my.twister.storm.bolts.stuff.LogBolt;
import my.twister.storm.spout.FileTestSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.TopologyBuilder;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Created by kkulagin on 10/23/2015.
 */
public class LocalFileTwitterTopology extends TwitterTopology {


  @Override
  protected IRichSpout createRootSpout() {
    return new FileTestSpout();
  }

  @Override
  protected Config config() {
    Config conf = super.config();
//    conf.put(Config.TOPOLOGY_DEBUG, true);

    conf.put(Config.NIMBUS_SEEDS, Lists.newArrayList("SYS10017.sysomos.pvt", "localhost", "127.0.0.1"));

    conf.put(FileTestSpout.TWEETS_FILE_LOCATION, "C:\\data\\twitter\\firehose_1460399483783.txt.gz");
    conf.put(FileTestSpout.TWEETS_RATE_LIMIT, 30000);
    conf.put(FileTestSpout.TWEETS_FILE_COMPRESSED, true);
    return conf;
  }

  @Override
  protected StormTopology topology() {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("rootSpout", createRootSpout(), 1);
    builder.setBolt("parserBolt", new ParserBolt(), 3).setNumTasks(2).shuffleGrouping("rootSpout");

    builder.setBolt("logProfileBolt", new LogBolt<Profile>(), 3).setNumTasks(3).shuffleGrouping("parserBolt", "profile");

    builder.setBolt("logTweetBolt", new LogBolt<Tweet>(), 3).setNumTasks(3).shuffleGrouping("parserBolt", "tweet");

    builder.setBolt("errorBolt", new ErrorBolt(), 1).setNumTasks(1).shuffleGrouping("parserBolt", "err");

    StormTopology topology = builder.createTopology();
    return topology;
  }

  public static void main(String[] args) throws IOException {

    LocalCluster cluster = new LocalCluster();

    TwitterTopology thisTopology = new LocalFileTwitterTopology();
    String topologyName = "sampleTwitterStream";
    cluster.submitTopology(topologyName, thisTopology.config(), thisTopology.topology());
    Uninterruptibles.sleepUninterruptibly(30, TimeUnit.SECONDS);
    cluster.killTopology(topologyName);
    cluster.shutdown();
  }

}

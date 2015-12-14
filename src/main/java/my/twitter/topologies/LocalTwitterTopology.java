package my.twitter.topologies;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.IRichSpout;
import com.google.common.util.concurrent.Uninterruptibles;
import my.twitter.beans.DeleteTweet;
import my.twitter.beans.Profile;
import my.twitter.beans.Tweet;
import my.twitter.spout.SampleTwitterSpout;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Created by kkulagin on 10/23/2015.
 */
public class LocalTwitterTopology extends TwitterTopology {

  public static void main(String[] args) throws IOException {

    LocalCluster cluster = new LocalCluster();

    LocalTwitterTopology thisTopology = new LocalTwitterTopology();
    String topologyName = "sampleTwitterStream";
    cluster.submitTopology(topologyName, thisTopology.config(), thisTopology.topology());
    Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
    cluster.killTopology(topologyName);
    cluster.shutdown();
  }

  @Override
  protected IRichSpout createRootSpout() {
    return new SampleTwitterSpout();
  }
}

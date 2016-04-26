package my.twister.storm.topologies;

import my.twister.utils.Constants;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;

import java.io.IOException;

/**
 * Created by kkulagin on 10/23/2015.
 */
public class RemoteSampleTwitterTopology extends SampleTwitterTopology {

  public static void main(String[] args) throws IOException {
    System.setProperty("storm.jar", "RemoteStorm.jar");

    RemoteSampleTwitterTopology thisTopology = new RemoteSampleTwitterTopology();
    try {
      StormSubmitter.submitTopology("fromSample", thisTopology.config(), thisTopology.topology());
    } catch (Exception e) {
      e.printStackTrace();
      System.err.println("Error submitting Topology" +  e.getMessage());
    }
  }

  @Override
  protected Config config() {
    Config conf = super.config();
    conf.put(Config.NIMBUS_HOST, "10.11.18.53");
    conf.put(Config.TOPOLOGY_NAME, "sampleTweets");
    conf.put(Constants.TWEET_INDEXER_HOST, "10.11.18.53");
    return conf;
  }


}

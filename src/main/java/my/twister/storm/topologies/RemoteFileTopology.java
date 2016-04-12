package my.twister.storm.topologies;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.IRichSpout;
import my.twister.storm.spout.FileTestSpout;
import my.twister.utils.Constants;

import java.io.IOException;

/**
 * Created by kkulagin on 10/23/2015.
 */
public class RemoteFileTopology extends TwitterTopology {

  public static void main(String[] args) throws IOException {
    System.setProperty("storm.jar", "RemoteStorm.jar");

    RemoteFileTopology thisTopology = new RemoteFileTopology();
    try {
      StormSubmitter.submitTopology("fromFile", thisTopology.config(), thisTopology.topology());
    } catch (Exception e) {
      e.printStackTrace();
      System.err.println("Error submitting Topology" +  e.getMessage());
    }
  }

  @Override
  protected IRichSpout createRootSpout() {
    return new FileTestSpout();
  }


  @Override
  protected Config config() {
    Config conf = super.config();
    conf.put(Config.NIMBUS_HOST, "10.11.18.53");
    conf.put(Config.TOPOLOGY_NAME, "fromFile");
    conf.put(Constants.TWEET_INDEXER_HOST, "10.11.18.53");

    conf.put(FileTestSpout.TWEETS_FILE_LOCATION, "/data/twitter/data.gz");
    conf.put(FileTestSpout.TWEETS_RATE_LIMIT, 10000);
    conf.put(FileTestSpout.TWEETS_FILE_COMPRESSED, true);

    return conf;
  }


}

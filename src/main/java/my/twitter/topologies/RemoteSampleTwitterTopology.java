package my.twitter.topologies;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import com.google.common.util.concurrent.Uninterruptibles;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Created by kkulagin on 10/23/2015.
 */
public class RemoteSampleTwitterTopology extends SampleTwitterTopology {

  public static void main(String[] args) throws IOException {
    System.setProperty("storm.jar", "RemoteStorm.jar");

    RemoteSampleTwitterTopology thisTopology = new RemoteSampleTwitterTopology();
    try {
      StormSubmitter.submitTopology("myTestTopology", thisTopology.config(), thisTopology.topology());
    } catch (Exception e) {
      e.printStackTrace();
      System.err.println("Error submitting Topology" +  e.getMessage());
    }
  }

  @Override
  protected Config config() {
    Config conf = super.config();
    conf.put(Config.NIMBUS_HOST, "10.11.18.53");
    return conf;
  }


}

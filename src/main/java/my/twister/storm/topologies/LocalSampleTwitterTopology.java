package my.twister.storm.topologies;

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.storm.LocalCluster;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Created by kkulagin on 10/23/2015.
 */
public class LocalSampleTwitterTopology extends SampleTwitterTopology {

  public static void main(String[] args) throws IOException {

    LocalCluster cluster = new LocalCluster();

    LocalSampleTwitterTopology thisTopology = new LocalSampleTwitterTopology();
    String topologyName = "sampleTwitterStream";
    cluster.submitTopology(topologyName, thisTopology.config(), thisTopology.topology());
    Uninterruptibles.sleepUninterruptibly(300, TimeUnit.SECONDS);
    cluster.killTopology(topologyName);
    cluster.shutdown();
  }

}

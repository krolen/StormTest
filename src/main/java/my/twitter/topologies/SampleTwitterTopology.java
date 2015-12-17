package my.twitter.topologies;

import backtype.storm.LocalCluster;
import backtype.storm.topology.IRichSpout;
import com.google.common.util.concurrent.Uninterruptibles;
import my.twitter.spout.SampleTwitterSpout;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Created by kkulagin on 10/23/2015.
 */
public abstract class SampleTwitterTopology extends TwitterTopology {

  @Override
  protected IRichSpout createRootSpout() {
    return new SampleTwitterSpout();
  }
}

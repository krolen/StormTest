package my.twister.storm.topologies;

import my.twister.storm.spout.SampleTwitterSpout;
import org.apache.storm.topology.IRichSpout;

/**
 * Created by kkulagin on 10/23/2015.
 */
public abstract class SampleTwitterTopology extends TwitterTopology {

  @Override
  protected IRichSpout createRootSpout() {

    return new SampleTwitterSpout();
  }
}

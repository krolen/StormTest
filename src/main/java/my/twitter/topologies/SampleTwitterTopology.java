package my.twitter.topologies;

import backtype.storm.topology.IRichSpout;
import my.twitter.spout.SampleTwitterSpout;

/**
 * Created by kkulagin on 10/23/2015.
 */
public abstract class SampleTwitterTopology extends TwitterTopology {

  @Override
  protected IRichSpout createRootSpout() {

    return new SampleTwitterSpout();
  }
}
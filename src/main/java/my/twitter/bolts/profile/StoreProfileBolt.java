package my.twitter.bolts.profile;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import my.twitter.beans.Profile;
import my.twitter.utils.LogAware;

/**
 * Created by kkulagin on 5/13/2015.
 */
public class StoreProfileBolt extends BaseBasicBolt implements LogAware {

  private long counter;

  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {
    Profile profile = (Profile) input.getValue(0);
    long l = counter++;
    if (l % 50 == 0) {
      log().debug("Storing profile " + profile.toString());
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

  }

}

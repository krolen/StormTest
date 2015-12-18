package my.twitter.bolts.profile;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import my.twitter.beans.Profile;
import my.twitter.utils.LogAware;

import java.util.Map;

/**
 * Created by kkulagin on 5/13/2015.
 */
public class StoreProfileBolt extends BaseRichBolt implements LogAware {

  private OutputCollector collector;
  private long counter;

  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
  }

  @Override
  public void execute(Tuple input) {
    Profile profile = (Profile) input.getValue(0);
      long l = counter++;
      if (l % 50 == 0) {
        log().debug("Storing profile " + profile.toString());
      }
      collector.ack(input);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

  }

}

package my.twister.storm.bolts.profile;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import my.twister.storm.beans.Profile;
import my.twister.utils.LogAware;

import java.util.Map;

/**
 * Created by kkulagin on 5/13/2015.
 */
public class ProfileLogBolt extends BaseRichBolt implements LogAware {

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
      if (l % 5 == 0) {
        log().debug(profile.toString());
      }
      collector.ack(input);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

  }

}

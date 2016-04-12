package my.twister.storm.bolts.stuff;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import my.twister.storm.beans.Tweet;
import my.twister.utils.LogAware;

/**
 * Created by kkulagin on 5/13/2015.
 */
public class LogBolt<T> extends BaseBasicBolt implements LogAware {

  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {
    T data = (T) input.getValue(0);
    log().info(data.toString());
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

  }

}
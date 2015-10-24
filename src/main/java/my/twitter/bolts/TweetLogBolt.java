package my.twitter.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import my.twitter.utils.LogAware;

import java.util.Map;

/**
 * Created by kkulagin on 5/13/2015.
 */
public class TweetLogBolt extends BaseRichBolt implements LogAware {

  private OutputCollector collector;
  private long counter;

  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
  }

  @Override
  public void execute(Tuple input) {
    String string = input.getString(0);
      long l = counter++;
      if (l % 50 == 0) {
        log().warn(string);
      }
      collector.ack(input);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

  }

}
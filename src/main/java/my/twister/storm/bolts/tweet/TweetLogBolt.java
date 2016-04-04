package my.twister.storm.bolts.tweet;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import my.twister.storm.beans.Tweet;
import my.twister.utils.LogAware;

/**
 * Created by kkulagin on 5/13/2015.
 */
public class TweetLogBolt extends BaseBasicBolt implements LogAware {

  private long counter;

  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {
    Tweet tweet = (Tweet) input.getValue(0);
      long l = counter++;
      if (l % 50 == 0) {
        log().debug(tweet.toString());
      }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

  }

}
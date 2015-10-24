package my.twitter.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.fasterxml.jackson.databind.ObjectMapper;
import my.twitter.beans.DeleteTweet;
import my.twitter.utils.LogAware;

import java.io.IOException;
import java.util.Map;

/**
 * Created by kkulagin on 5/13/2015.
 */
public class DeleteTweetLogBolt extends BaseRichBolt implements LogAware {

  private OutputCollector collector;
  private long counter;
  private ObjectMapper objectMapper;

  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
    objectMapper = new ObjectMapper();
  }

  // TODO: 24.10.2015 check Ask
  @Override
  public void execute(Tuple input) {
    try {
      byte[] deleteTweetBytes = input.getBinaryByField("deleteTweet");
      DeleteTweet deleteTweet = objectMapper.readValue(deleteTweetBytes, 10, deleteTweetBytes.length - 11, DeleteTweet.class);
      long l = counter++;
      if (l % 2 == 0) {
        log().warn(deleteTweet.toString());
      }
      collector.ack(input);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

  }

}

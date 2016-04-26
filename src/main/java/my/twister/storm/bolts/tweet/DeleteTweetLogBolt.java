package my.twister.storm.bolts.tweet;

import com.fasterxml.jackson.databind.ObjectMapper;
import my.twister.storm.beans.DeleteTweet;
import my.twister.utils.LogAware;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;
import java.util.Map;

/**
 * Created by kkulagin on 5/13/2015.
 */
public class DeleteTweetLogBolt extends BaseRichBolt implements LogAware {

  private OutputCollector collector;
  private transient long counter;
  private transient ObjectMapper objectMapper;

  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
    objectMapper = new ObjectMapper();
    counter = 0;
  }

  // TODO: 24.10.2015 check Ask
  @Override
  public void execute(Tuple input) {
    try {
      byte[] deleteTweetBytes = input.getBinaryByField("deleteTweet");
      DeleteTweet deleteTweet = objectMapper.readValue(deleteTweetBytes, 10, deleteTweetBytes.length - 11, DeleteTweet.class);
      long l = counter++;
      if (l % 50 == 0) {
        log().warn(deleteTweet.toString());
      }
      collector.ack(input);
    } catch (IOException e) {
      // TODO Kostya Kulagin Handle error correctly
      e.printStackTrace();
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

  }

}

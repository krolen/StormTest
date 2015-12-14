package my.twitter.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.fasterxml.jackson.databind.ObjectMapper;
import my.twitter.beans.Tweet;
import my.twitter.utils.LogAware;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;

/**
 * Created by kkulagin on 5/15/2015.
 */
public class ParserBolt extends BaseRichBolt implements LogAware {

  private ObjectMapper objectMapper = new ObjectMapper();
  private OutputCollector collector;

  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
  }

  @Override
  public void execute(Tuple input) {
    byte[] binaryInput = input.getBinaryByField("wholeTweet");
    try {
      if(binaryInput[2] == 100) {
        // delete tweet
        log().debug("Emitting deleted tweet.");
        collector.emit("deleteTweet", input, new Values(binaryInput));
      } else {
        Tweet tweet = objectMapper.readValue(binaryInput, Tweet.class);
        collector.emit("profile", input, new Values(tweet.getUser()));
        tweet.prepareForSerialization();
        collector.emit("tweet", input, new Values(tweet));
      }
    } catch (Exception e) {
      log().error("Error parsing tweet", e);
      try {
        StringWriter error = new StringWriter();
        e.printStackTrace(new PrintWriter(error));
        collector.emit("err", input, new Values("{ Error : " + error.toString() + ", Tweet : " + new String(binaryInput, "UTF-8")));
      } catch (Exception e1) {
        e1.printStackTrace();
        log().error("Error sending error", e1);
      }
    }
    collector.ack(input);
  }

  @Override
  public void cleanup() {
    log().warn("Closing");
    super.cleanup();
    log().warn("Closed");
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declareStream("profile", new Fields("profile"));
    declarer.declareStream("tweet", new Fields("tweet"));
    declarer.declareStream("deleteTweet", new Fields("deleteTweet"));
    declarer.declareStream("err", new Fields("err"));
//    declarer.declare(new Fields("tweet"));
  }
}

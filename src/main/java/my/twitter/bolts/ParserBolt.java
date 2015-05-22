package my.twitter.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import my.twitter.beans.Profile;
import my.twitter.beans.Tweet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;

/**
 * Created by kkulagin on 5/15/2015.
 */
public class ParserBolt extends BaseRichBolt {

  private static final Logger logger = LoggerFactory.getLogger(ParserBolt.class);

  private ObjectMapper objectMapper = new ObjectMapper();
  private OutputCollector collector;

  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
  }

  @Override
  public void execute(Tuple input) {
    byte[] binaryInput = input.getBinaryByField("WholeTweet");
    try {
      Tweet tweet = objectMapper.readValue(binaryInput, Tweet.class);
      Profile profile = tweet.getUser();
      collector.emit("profile", input, new Values(objectMapper.writeValueAsString(profile)));
      collector.emit(input, new Values(objectMapper.writeValueAsString(tweet)));
    } catch (Exception e) {
      logger.error("Error parsing tweet", e);
      try {
        StringWriter error = new StringWriter();
        e.printStackTrace(new PrintWriter(error));
        collector.emit("err", input, new Values("{ Error : " + error.toString() + ", Tweet : " + new String(binaryInput, "UTF-8")));
      } catch (Exception e1) {
        e1.printStackTrace();
        logger.error("Error sending error", e1);
      }
    }
    collector.ack(input);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declareStream("profile", new Fields("profile"));
    declarer.declareStream("err", new Fields("err"));
    declarer.declare(new Fields("tweet"));
  }
}

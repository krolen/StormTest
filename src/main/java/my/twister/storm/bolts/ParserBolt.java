package my.twister.storm.bolts;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.fasterxml.jackson.databind.ObjectMapper;
import my.twister.storm.beans.Tweet;
import my.twister.utils.LogAware;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by kkulagin on 5/15/2015.
 */
public class ParserBolt extends BaseBasicBolt implements LogAware {

  private transient ObjectMapper objectMapper;
  private transient AtomicLong counter;
  private transient int thisTaskIndex;

  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    super.prepare(stormConf, context);
    objectMapper = new ObjectMapper();
    counter = new AtomicLong(0);
    thisTaskIndex = context.getThisTaskIndex();
  }

  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {
    byte[] binaryInput = input.getBinaryByField("wholeTweet");
    try {
      if(binaryInput[2] == 100) {
        // delete tweet
        log().debug("Emitting deleted tweet.");
        collector.emit("deleteTweet", new Values(binaryInput));
      } else {
        // TODO: 3/29/2016 optimize to skip unnecessary values using jackson stream reader
        Tweet tweet = objectMapper.readValue(binaryInput, Tweet.class);
//        if(tweet.getUser().getId() > 230835570100L) {
//          collector.emit("anomaly", new Values(binaryInput));
//        }
        collector.emit("profile", new Values(tweet.getUser()));
        tweet.prepareForSerialization();
        collector.emit("tweet", new Values(tweet));
        if(counter.incrementAndGet() % 100_000 == 0) {
          log().info("Parsed by " + thisTaskIndex + " task: " + counter.get());
        }
      }
    } catch (Exception e) {
      log().error("Error parsing tweet", e);
      try {
        StringWriter error = new StringWriter();
        e.printStackTrace(new PrintWriter(error));
        collector.emit("err", new Values("{ Error : " + error.toString() + ", Tweet : " + new String(binaryInput, "UTF-8")));
      } catch (Exception e1) {
        e1.printStackTrace();
        log().error("Error sending error", e1);
      }
    }
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
    declarer.declareStream("anomaly", new Fields("data"));
    declarer.declareStream("deleteTweet", new Fields("deleteTweet"));
    declarer.declareStream("err", new Fields("err"));
//    declarer.declare(new Fields("tweet"));
  }
}

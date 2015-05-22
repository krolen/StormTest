package my.twitter.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.fasterxml.jackson.databind.ObjectMapper;
import my.twitter.beans.Tweet;
import org.apache.storm.guava.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by kkulagin on 5/15/2015.
 */
public class ErrorBolt extends BaseRichBolt {

  private static final Logger logger = LoggerFactory.getLogger(ErrorBolt.class);
  private OutputCollector collector;

  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
  }

  @Override
  public void execute(Tuple input) {
    String string = input.getString(0);
    if (!Strings.isNullOrEmpty(string)) {
      logger.error(string);
      collector.ack(input);
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
  }
}

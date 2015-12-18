package my.twitter.bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.google.common.base.Strings;
import my.twitter.utils.LogAware;

/**
 * Created by kkulagin on 5/15/2015.
 */
public class ErrorBolt extends BaseBasicBolt implements LogAware {

  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {
    String string = input.getString(0);
    if (!Strings.isNullOrEmpty(string)) {
      log().error(string);
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
  }
}

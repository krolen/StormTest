package my.twister.storm.bolts;

import com.google.common.base.Strings;
import my.twister.utils.LogAware;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

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

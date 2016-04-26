package my.twister.storm.bolts.stuff;

import my.twister.utils.LogAware;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

/**
 * Created by kkulagin on 5/13/2015.
 */
public class AnomalyLogBolt extends BaseBasicBolt implements LogAware {

  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {
    try {
      byte[] anomaly = input.getBinaryByField("data");
      log().warn("Anomaly detected: " + new String(anomaly));
    } catch (Exception e) {
      log().error("WTF?", e);
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

  }

}
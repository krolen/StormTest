package my.twister.storm.bolts.stuff;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import my.twister.storm.beans.Tweet;
import my.twister.utils.LogAware;

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
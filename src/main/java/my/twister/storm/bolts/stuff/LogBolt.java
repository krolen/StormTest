package my.twister.storm.bolts.stuff;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import my.twister.storm.beans.Tweet;
import my.twister.utils.LogAware;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by kkulagin on 5/13/2015.
 */
public class LogBolt<T> extends BaseBasicBolt implements LogAware {

  private transient AtomicLong counter;
  private int thisTaskIndex;

  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    super.prepare(stormConf, context);
    thisTaskIndex = context.getThisTaskIndex();
    counter = new AtomicLong(0);
  }

  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {
    T data = (T) input.getValue(0);
    if (counter.incrementAndGet() % 1000 == 0) {
      log().info("For class " + data.getClass() + " at task number " + thisTaskIndex +" logged: " + counter.get());
//      log().info(data.toString());
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

  }

}
package my.twister.storm.bolts.stuff;

import my.twister.utils.LogAware;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

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
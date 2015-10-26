package my.twitter.bolts.profile.chronicle;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import my.twitter.beans.Profile;
import my.twitter.utils.LogAware;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * Created by kkulagin on 10/26/2015.
 */
public class Name2IdBolt extends BaseRichBolt implements LogAware {

  private OutputCollector collector;
  private ChronicleMap<String, Long> map;

  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
    String fileLocation = (String) stormConf.get("profile.name.to.id.file");
    File file = new File(fileLocation);
    ChronicleMapBuilder<String, Long> builder =
        ChronicleMapBuilder.of(String.class, Long.class).
            averageKeySize("this_is_18_charctr".getBytes().length).
            entries(1000);
    try {
      map = builder.createPersistedTo(file);
    } catch (IOException e) {
      // fail fast
      throw new RuntimeException(e);
    }
  }

  @Override
  public void execute(Tuple input) {
    Profile profile = (Profile) input.getValue(0);
    map.update(profile.getScreenName(), profile.getId());
    log().info("Map size is " + map.longSize());
    collector.ack(input);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

  }

  @Override
  public void cleanup() {
    map.close();
    super.cleanup();
  }
}

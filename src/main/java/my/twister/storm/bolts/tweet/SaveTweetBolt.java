package my.twister.storm.bolts.tweet;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import my.twister.chronicle.ChronicleDataService;
import my.twister.entities.IShortTweet;
import my.twister.storm.beans.Tweet;
import my.twister.storm.bolts.profile.chronicle.StormCDSSingletonWrapper;
import my.twister.utils.LogAware;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.values.Values;

import java.util.Map;

/**
 * Created by kkulagin on 5/13/2015.
 */
public class SaveTweetBolt extends BaseBasicBolt implements LogAware {

  private transient LongValue tweetId;
  private transient IShortTweet tweet;

  private ChronicleDataService chronicleDataService;


  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    super.prepare(stormConf, context);
    chronicleDataService = StormCDSSingletonWrapper.getInstance();
    chronicleDataService.connect(3);
    tweet = Values.newHeapInstance(IShortTweet.class);
    tweetId = Values.newHeapInstance(LongValue.class);
  }

  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {
    Tweet t = (Tweet) input.getValue(0);
    ChronicleMap<LongValue, IShortTweet> tweetsDataMap = chronicleDataService.getTweetsDataMap(t.getId());
    if(tweetsDataMap == null) {
      log().error("Cannot find storage for tweet " + t.getId());
    } else {
      tweetId.setValue(t.getId());

      tweet.setMentions(t.getMentions());
      tweet.setAuthorId(t.getAuthorId());
      tweet.setCreateDate(t.getCreateDate());
      tweetsDataMap.put(tweetId, tweet);
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

  }

  @Override
  public void cleanup() {
    chronicleDataService.close();
    super.cleanup();
  }

}
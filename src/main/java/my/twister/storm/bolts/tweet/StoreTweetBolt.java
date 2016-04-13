package my.twister.storm.bolts.tweet;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import my.twister.chronicle.ChronicleDataService;
import my.twister.entities.IShortTweet;
import my.twister.storm.beans.Tweet;
import my.twister.utils.LogAware;
import my.twister.utils.Utils;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.values.Values;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Created by kkulagin on 5/13/2015.
 */
public class StoreTweetBolt extends BaseBasicBolt implements LogAware {

  private transient LongValue tweetId;
  private transient IShortTweet tweet;

  private transient ChronicleDataService chronicleDataService;


  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    super.prepare(stormConf, context);
    chronicleDataService = ChronicleDataService.getInstance();
    chronicleDataService.connectTweetsMaps(3);
    tweet = Values.newHeapInstance(IShortTweet.class);
    tweetId = Values.newHeapInstance(LongValue.class);
    long millisPerHour = Duration.ofHours(1).toMillis();
  }

  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {
    Tweet t = (Tweet) input.getValue(0);
    long closestHourMillis = Utils.getClosestHourStart(t.getCreateDate());
    ChronicleMap<LongValue, IShortTweet> tweetsDataMap = chronicleDataService.getTweetsDataMap(closestHourMillis);
    if(tweetsDataMap == null) {
      log().error("Cannot find storage for tweet " + t);
    } else {
      tweetId.setValue(t.getId());

      long[] mentions = t.getMentions();
      if(mentions != null) {
        for (int i = 0; i < mentions.length; i++) {
          long mention = mentions[i];
          tweet.setMentionAt(i, mention);
        }
      }
      tweet.setAuthorId(t.getAuthorId());
      tweet.setCreateDate(t.getCreateDate());
      tweet.setRetweetedTweetUserId(t.getRetweetedTweetUserId().longValue());
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
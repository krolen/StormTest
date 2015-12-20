package my.twitter.bolts.tweet;

import backtype.storm.metric.api.CountMetric;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.google.common.primitives.Longs;
import my.twitter.beans.Tweet;
import my.twitter.bolts.profile.chronicle.ChronicleDataService;
import my.twitter.utils.LogAware;
import net.openhft.chronicle.map.ChronicleMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by kkulagin on 5/13/2015.
 */
public class TweetMentionsBolt extends BaseBasicBolt implements LogAware {
  private static final Pattern SCREEN_NAME_PATTERN = Pattern.compile("[@＠][a-zA-Z0-9_]+");

  private transient MultiCountMetric mentionsMetric;
  private transient long counter;
  private transient ChronicleMap<String, Long> name2IdMap;

  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    super.prepare(stormConf, context);
    name2IdMap = ChronicleDataService.getInstance(stormConf).getName2IdMap();
    mentionsMetric = new MultiCountMetric();

    context.registerMetric("mentions", mentionsMetric, 60);
  }

  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {
    Tweet tweet = (Tweet) input.getValue(0);

    List<Long> mentions = new ArrayList<>();
    String contents = tweet.getContents();
    Matcher matcher = SCREEN_NAME_PATTERN.matcher(contents);
    int mentionsCount = 0;
    while (matcher.find()) {
      mentionsCount++;
      String mention = contents.substring(matcher.start() + 1, matcher.end());
      Long id = name2IdMap.get(mention.toLowerCase());
      if (id == null) {
        mentionsMetric.scope("missed_mentions").incr();
      } else {
        mentions.add(id);
      }
    }
    if(mentionsCount > 0) {
      int size = mentions.size();
      log().warn("Resolved " + size + " of " + mentionsCount + " mentions.");
      if(size > 0) {
        mentionsMetric.scope("extracted_mentions").incrBy(size);
        tweet.setMentions(Longs.toArray(mentions));
      }
    }
    long l = counter++;
    if (l % 50 == 0) {
      log().debug(tweet.toString());
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

  }

}
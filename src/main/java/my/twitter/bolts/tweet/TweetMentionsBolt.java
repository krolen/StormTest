package my.twitter.bolts.tweet;

import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import my.twitter.beans.Tweet;
import my.twitter.bolts.profile.chronicle.ChronicleDataService;
import my.twitter.utils.LogAware;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.values.Values;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by kkulagin on 5/13/2015.
 */
public class TweetMentionsBolt extends BaseBasicBolt implements LogAware {
  private static final Pattern SCREEN_NAME_PATTERN = Pattern.compile("[@＠][a-zA-Z0-9_]+");

  private transient MultiCountMetric mentionsMetric;
  private transient ChronicleMap<CharSequence, LongValue> name2IdMap;
  private transient StringBuilder mentionBuffer = new StringBuilder();
  private transient int count = 0;
  private transient LongValue profileIdValue;
  private transient int resolvedMentionsCount;
  private transient long[] mentions;

  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    super.prepare(stormConf, context);
    name2IdMap = ChronicleDataService.getInstance(stormConf).getName2IdMap();
    mentionsMetric = new MultiCountMetric();
    mentionBuffer = new StringBuilder();
    count = 0;
    profileIdValue = Values.newHeapInstance(LongValue.class);
    resolvedMentionsCount = 0;
    mentions = new long[50];

    context.registerMetric("mentions", mentionsMetric, 60);
  }

  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {
    Tweet tweet = (Tweet) input.getValue(0);

    String contents = tweet.getContents();
    Matcher matcher = SCREEN_NAME_PATTERN.matcher(contents);
    resolvedMentionsCount = 0;
    while (matcher.find()) {
      mentionBuffer.setLength(0);
      for (count = matcher.start() + 1; count < matcher.end(); count++) {
        mentionBuffer.append(Character.toLowerCase(contents.charAt(count)));
      }
      profileIdValue = name2IdMap.get(mentionBuffer);
      if (profileIdValue == null) {
        mentionsMetric.scope("missed_mentions").incr();
      } else {
        mentions[resolvedMentionsCount] = profileIdValue.getValue();
        resolvedMentionsCount++;
      }
    }

//    log().warn("Resolved " + resolvedMentionsCount + " of " + mentionsCount + " mentions.");
    if(resolvedMentionsCount > 0) {
        mentionsMetric.scope("extracted_mentions").incrBy(resolvedMentionsCount);
        long[] result = new long[resolvedMentionsCount - 1];
        System.arraycopy(mentions, 0, result, 0, resolvedMentionsCount - 1);
        tweet.setMentions(result);
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

  }

  @Override
  public void cleanup() {
    name2IdMap.close();
    super.cleanup();
  }

}
package my.twister.storm.bolts.tweet;

import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import my.twister.chronicle.ChronicleDataService;
import my.twister.storm.beans.Tweet;
import my.twister.storm.bolts.profile.chronicle.StormCDSSingletonWrapper;
import my.twister.utils.LogAware;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.values.Values;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by kkulagin on 5/13/2015.
 */
public class SaveTweetBolt extends BaseBasicBolt implements LogAware {
  private static final Pattern SCREEN_NAME_PATTERN = Pattern.compile("[@＠][a-zA-Z0-9_]+");

  private transient MultiCountMetric mentionsMetric;
  private transient ChronicleMap<CharSequence, LongValue> name2IdMap;
  private transient StringBuilder mentionBuffer = new StringBuilder();
  public static final int MAX_MENTIONS = 10;
  private transient int count = 0;
  private transient LongValue profileIdValue;
  private transient int resolvedMentionsCount;
  private transient long[] mentions;
  private ChronicleDataService chronicleDataService;

  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    super.prepare(stormConf, context);
    chronicleDataService = StormCDSSingletonWrapper.getInstance();
    chronicleDataService.connect(4);
    name2IdMap = chronicleDataService.getName2IdMap();

    mentionsMetric = new MultiCountMetric();
    mentionBuffer = new StringBuilder();
    count = 0;
    profileIdValue = Values.newHeapInstance(LongValue.class);
    resolvedMentionsCount = 0;
    mentions = new long[10];

    context.registerMetric("mentions", mentionsMetric, 60);
  }

  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {
    Tweet tweet = (Tweet) input.getValue(0);

    String contents = tweet.getContents();
    Matcher matcher = SCREEN_NAME_PATTERN.matcher(contents);
    resolvedMentionsCount = 0;
    while (matcher.find() && resolvedMentionsCount < MAX_MENTIONS) {
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
    chronicleDataService.close();
    super.cleanup();
  }

}
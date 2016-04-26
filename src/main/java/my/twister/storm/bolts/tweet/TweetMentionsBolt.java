package my.twister.storm.bolts.tweet;

import my.twister.chronicle.ChronicleDataService;
import my.twister.storm.beans.Tweet;
import my.twister.utils.Constants;
import my.twister.utils.LogAware;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.values.Values;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by kkulagin on 5/13/2015.
 */
public class TweetMentionsBolt extends BaseBasicBolt implements LogAware {
  private static final Pattern SCREEN_NAME_PATTERN = Pattern.compile("[@＠][a-zA-Z0-9_]+");

  private transient MultiCountMetric mentionsMetric;
  private transient StringBuilder mentionBuffer = new StringBuilder();
  private transient int count = 0;
  private transient LongValue profileIdValue;
  private transient int resolvedMentionsCount;
  private transient long[] mentions;
  private transient ChronicleDataService chronicleDataService;
  private transient ChronicleDataService.MapReference<CharSequence, LongValue> name2IdMapReference;

  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    super.prepare(stormConf, context);
    chronicleDataService = ChronicleDataService.getInstance();
    name2IdMapReference = chronicleDataService.connectName2IdMap();

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
    if(contents != null ) {
      Matcher matcher = SCREEN_NAME_PATTERN.matcher(contents);
      resolvedMentionsCount = 0;
      while (matcher.find() && resolvedMentionsCount < Constants.MAX_MENTIONS_SIZE) {
        mentionBuffer.setLength(0);
        for (count = matcher.start() + 1; count < matcher.end(); count++) {
          mentionBuffer.append(Character.toLowerCase(contents.charAt(count)));
        }
        profileIdValue = name2IdMapReference.map().get(mentionBuffer);
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
    collector.emit("tweet", new org.apache.storm.tuple.Values(tweet));

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declareStream("tweet", new Fields("tweet"));
  }

  @Override
  public void cleanup() {
    chronicleDataService.release(name2IdMapReference);
    super.cleanup();
  }

}
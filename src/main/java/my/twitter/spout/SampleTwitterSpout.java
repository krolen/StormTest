package my.twitter.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import my.twitter.utils.LogAware;
import twitter4j.RawStreamListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Created by kkulagin on 10/23/2015.
 */
public class SampleTwitterSpout extends BaseRichSpout implements LogAware {

  private TwitterStream twitterStream;
  private Queue<String> tweetsCache = new ArrayBlockingQueue<>(1000);
  private SpoutOutputCollector collector;

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("tweet"));
  }


  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    this.collector = collector;
    setup();
  }

  @Override
  public void activate() {
    super.activate();
    twitterStream.sample();
  }

  @Override
  public void nextTuple() {
    String tweet = tweetsCache.poll();
    if(tweet != null) {
      collector.emit(new Values(tweet));
    }
  }

  @Override
  public void close() {
    twitterStream.shutdown();
    super.close();
  }

  private void setup() {
    RawStreamListener listener = new RawStreamListener() {
      @Override
      public void onMessage(String rawString) {
        if(!tweetsCache.offer(rawString)){
          log().warn("Cache is full, skipping");
        }
      }

      @Override
      public void onException(Exception ex) {
        log().error("Error reading sample tweets stream");
        collector.reportError(ex);
      }
    };

    TwitterStreamFactory factory = new TwitterStreamFactory();
    twitterStream = factory.getInstance();
    twitterStream.addListener(listener);
  }

  public static void main(String[] args) {
    new SampleTwitterSpout().setup();
  }
}

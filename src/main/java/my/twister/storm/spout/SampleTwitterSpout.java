package my.twister.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.google.common.util.concurrent.Uninterruptibles;
import my.twister.utils.LogAware;
import twitter4j.RawStreamListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by kkulagin on 10/23/2015.
 */
public class SampleTwitterSpout extends BaseRichSpout implements LogAware {

  private transient TwitterStream twitterStream;
  private transient Queue<String> tweetsCache;
  private transient SpoutOutputCollector collector;
  private transient AtomicInteger counter;
  private transient AtomicLong ids;

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("wholeTweet"));
  }


  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    this.collector = collector;
    tweetsCache = new ArrayBlockingQueue<>(100);
    counter = new AtomicInteger(0);
    ids = new AtomicLong(1);
    setup(conf);
  }

  @Override
  public void activate() {
    super.activate();
    twitterStream.sample();
  }

  @Override
  public void nextTuple() {
    String tweet = tweetsCache.poll();
    if (tweet != null) {
      collector.emit(new Values(tweet.getBytes(StandardCharsets.UTF_8)), ids.getAndIncrement());
      int i = counter.incrementAndGet();
      if (i % 100_000 == 0) {
        log().debug("Consumed " + i + " tweets.");
      }
    }
  }

  @Override
  public void ack(Object msgId) {
    super.ack(msgId);
  }

  @Override
  public void close() {
    log().warn("Shutting down stream...");
    twitterStream.shutdown();
    log().warn("Done...");
    int count = 0;
    while (tweetsCache.size() > 0 && count++ < 10) {
      log().warn("Waiting for empty buffer");
      Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
    }
    log().warn("Closed");
    super.close();
  }

  private void setup(Map conf) {
    // init to avoid timeout
    RawStreamListener listener = new RawStreamListener() {
      @Override
      public void onMessage(String rawString) {
//        log().debug("rawJsonTweet: " + rawString);
        if (!tweetsCache.offer(rawString)) {
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
    new SampleTwitterSpout().setup(new HashMap<>());
  }
}

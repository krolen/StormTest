package my.twitter.bolts.tweet;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import my.twitter.beans.Tweet;
import my.twitter.utils.Constants;
import my.twitter.utils.LogAware;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.websocket.api.RemoteEndpoint;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by kkulagin on 5/13/2015.
 */
public class TweetIndexerBolt extends BaseBasicBolt implements LogAware {

  public static final int INDEXERS_NUMBER = 3;
  private static final long MILLIS_PER_HOUR = Duration.ofHours(1).toMillis();
  private static final long HOUR_SHIFT = 0xFF_FF_FF_FF_FF_FF_FF_E0L;

  private WebSocketClient client;
  private Map<Long, RemoteEndpoint> time2Indexer = new HashMap<>();

  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    System.setProperty("org.eclipse.jetty.websocket.LEVEL", "INFO");

    client = new WebSocketClient();
    try {
      QueuedThreadPool threadPool = new QueuedThreadPool(20, 8);
      threadPool.setName(WebSocketClient.class.getSimpleName() + "@" + hashCode());
      threadPool.setDaemon(true);
      client.setExecutor(threadPool);
      client.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    Instant now = Instant.now();
    LocalDateTime dateTime = LocalDateTime.ofInstant(now, ZoneId.systemDefault());
    LocalDateTime dayStart = dateTime.with(LocalTime.MIN);
    LocalDateTime dayBefore = dayStart.minusDays(1);
    long dayBeforeStartInMillis = dayBefore.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();

    for (int i = 0; i < INDEXERS_NUMBER; i++) {
      String indexerUrl = getIndexerUrl(stormConf, i);
      log().info("Establishing web socket connection to " + indexerUrl);
      RemoteEndpoint remoteEndpoint = openSocket(indexerUrl);
      log().info("Connection established");
      long start = dayBeforeStartInMillis + i * MILLIS_PER_HOUR;
      // TODO: 3/31/2016 make it dynamic - for now it will fill around 100 days
      for (int j = 0; j < 1000; j++) {
        time2Indexer.put(start + j * INDEXERS_NUMBER * MILLIS_PER_HOUR, remoteEndpoint);
      }
    }

    log().info("TweetIndexerBolt started");
  }

  @NotNull
  private String getIndexerUrl(Map stormConf, int indexerId) {
    String indexerHost = Optional.ofNullable((String) stormConf.get(Constants.TWEET_INDEXER_HOST)).orElse("localhost");
    long indexerPort = Optional.ofNullable((Long) stormConf.get(Constants.TWEET_INDEXER_PORT)).orElse(8880L) + indexerId;
    return "ws://" + indexerHost + ":" + indexerPort + "/textSaveTweet";
  }

  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {
    Tweet tweet = (Tweet) input.getValue(0);
    long createDate = tweet.getCreateDate();
    long prevHour = createDate & HOUR_SHIFT;
    long prevPrevHour = prevHour - MILLIS_PER_HOUR;
    RemoteEndpoint prevHourEndpoint = time2Indexer.get(prevHour);
    RemoteEndpoint prevPrevHourEndpoint = time2Indexer.get(prevPrevHour);
    send(tweet, prevHourEndpoint);
    send(tweet, prevPrevHourEndpoint);
  }

  private void send(Tweet tweet, RemoteEndpoint remote) {
    try {
      remote.sendString(tweet.getId() + "|" + tweet.getCreateDate() + "|" + tweet.getContents());
    } catch (Exception e) {
      log().error("Error sending message to indexer ", e);
    }
  }

  private RemoteEndpoint openSocket(String destUri) {
    log().info("Connecting to : %s%n", destUri);
    SimpleSocket socket = new SimpleSocket();
    try {
      client.connect(socket, new URI(destUri), new ClientUpgradeRequest());
      if (!socket.getOpenLatch().await(5, TimeUnit.SECONDS)) {
        throw new RuntimeException("Cannot connect to a server!!!!");
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    Session session = socket.getSession();
    return session.getRemote();
  }


  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

  }

  @Override
  public void cleanup() {
    log().info("Closing");
    Optional.ofNullable(client).ifPresent((c) -> {
      try {
        c.stop();
      } catch (Exception e) {
        log().error("Error stopping websocket connection", e);
      }
    });
    super.cleanup();
    log().info("Closed");
  }

  @WebSocket(maxTextMessageSize = 64 * 1024, maxIdleTime = 6 * 60 * 60 * 1000)
  public static class SimpleSocket implements LogAware {

    private final CountDownLatch closeLatch;
    private final CountDownLatch openLatch;

    @SuppressWarnings("unused")
    private Session session;

    public SimpleSocket() {
      this.closeLatch = new CountDownLatch(1);
      this.openLatch = new CountDownLatch(1);
    }

    public boolean awaitClose(int duration, TimeUnit unit) throws InterruptedException {
      return this.closeLatch.await(duration, unit);
    }

    @OnWebSocketClose
    public void onClose(int statusCode, String reason) {
      log().debug("Connection closed: %d - %s%n", statusCode, reason);
      this.session = null;
      this.closeLatch.countDown();
    }

    @OnWebSocketConnect
    public void onConnect(Session session) {
      log().debug("Got connect: %s%n", session);
      this.session = session;
      openLatch.countDown();
    }

    @OnWebSocketMessage
    public void onMessage(String msg) {
      log().info("Got msg: %s%n", msg);
    }

    public Session getSession() {
      return session;
    }

    public CountDownLatch getOpenLatch() {
      return openLatch;
    }
  }

}
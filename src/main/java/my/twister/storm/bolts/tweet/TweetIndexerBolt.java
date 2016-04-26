package my.twister.storm.bolts.tweet;

import my.twister.storm.beans.Tweet;
import my.twister.utils.Constants;
import my.twister.utils.LogAware;
import my.twister.utils.Utils;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
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
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by kkulagin on 5/13/2015.
 */
public class TweetIndexerBolt extends BaseBasicBolt implements LogAware {

  public static final int INDEXERS_NUMBER = 3;

  private transient WebSocketClient client;
  private NavigableMap<Long, RemoteEndpoint> time2Indexer = new TreeMap<>();

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
      long start = dayBeforeStartInMillis + i * Utils.MILLIS_PER_HOUR;
      log().info("Connection established");
      // TODO: 3/31/2016 make it dynamic - for now it will fill around 100 days
      for (int j = 0; j < 10000; j++) {
        time2Indexer.put(start + j * INDEXERS_NUMBER * Utils.MILLIS_PER_HOUR, remoteEndpoint);
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
    Map.Entry<Long, RemoteEndpoint> floorEntry = time2Indexer.floorEntry(createDate);
    if(floorEntry == null) {
      log().error("Cannot find time entry for tweet " + tweet);
    } else {
      RemoteEndpoint prevHourEndpoint = floorEntry.getValue();
      RemoteEndpoint prevPrevHourEndpoint = time2Indexer.get(floorEntry.getKey() - Utils.MILLIS_PER_HOUR);
      send(tweet, prevHourEndpoint);
      send(tweet, prevPrevHourEndpoint);
    }
  }

  private void send(Tweet tweet, RemoteEndpoint remote) {
    Exception exception = null;
    try {
      remote.sendString(tweet.getId() + "|" + tweet.getCreateDate() + "|" + tweet.getContents());
    } catch (IOException e) {
      log().error("Error sending message to indexer ", e);
      exception = e;
    } catch (Exception e) {
      log().error("Error sending message to indexer ", e);
    }
    if(exception != null) {
      throw new RuntimeException(exception);
    }
  }

  private RemoteEndpoint openSocket(String destUri) {
    log().info("Connecting to :" + destUri);
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
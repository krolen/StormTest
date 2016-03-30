package my.twitter.bolts.tweet;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import com.google.common.collect.ImmutableMap;
import my.twitter.beans.Tweet;
import my.twitter.utils.Constants;
import my.twitter.utils.LogAware;
import org.apache.storm.shade.com.google.common.collect.ImmutableList;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.websocket.api.RemoteEndpoint;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by kkulagin on 5/13/2015.
 */
public class TweetIndexerBolt extends BaseBasicBolt implements LogAware {

  private WebSocketClient client;
  private SimpleSocket socket;
  private RemoteEndpoint remote;
  private Session session;

  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    System.setProperty("org.eclipse.jetty.websocket.LEVEL", "INFO");
    String thisComponentId = context.getThisComponentId();
    int indexerId = Integer.parseInt(thisComponentId.substring(thisComponentId.length() - 1));
    String indexerHost = Optional.ofNullable((String) stormConf.get(Constants.TWEET_INDEXER_HOST)).orElse("localhost");
    int indexerPort = Optional.ofNullable((Integer) stormConf.get(Constants.TWEET_INDEXER_PORT)).orElse(8080) + indexerId;
    String destUri = "ws://" + indexerHost + ":" + indexerPort + "/textSaveTweet";
    log().info("Setting web socket connection to " + destUri);

    client = new WebSocketClient();
    socket = new SimpleSocket();

    try {
      QueuedThreadPool threadPool = new QueuedThreadPool(5, 1);
      String name = WebSocketClient.class.getSimpleName() + "@" + hashCode();
      threadPool.setName(name);
      threadPool.setDaemon(true);
      client.setExecutor(threadPool);
      client.start();
      URI echoUri = new URI(destUri);
      ClientUpgradeRequest request = new ClientUpgradeRequest();
      System.out.printf("Connecting to : %s%n", echoUri);
      client.connect(socket, echoUri, request);
      if (!socket.getOpenLatch().await(5, TimeUnit.SECONDS)) {
        throw new RuntimeException("Cannot connect to a server!!!!");
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    session = socket.getSession();
    remote = session.getRemote();

    log().info("Connected");
  }

  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {
    Tweet tweet = (Tweet) input.getValue(0);
    try {
      remote.sendString(tweet.getId() + "|" + tweet.getCreateDate() + "|" + tweet.getContents());
    } catch (IOException e) {
      log().error("Error sending message to indexer ", e);
    }
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
      log().debug("Got msg: %s%n", msg);
    }

    public Session getSession() {
      return session;
    }

    public CountDownLatch getOpenLatch() {
      return openLatch;
    }
  }

  public static void main(String[] args) {
    TweetIndexerBolt bolt = new TweetIndexerBolt();

    try {
      bolt.prepare(new HashMap<>(), new TopologyContext(null, null, ImmutableMap.of(5, "tweetIndexer0"), null, null, null, null, null, 5,
          null, null, null, null, null, null, null));

      Tweet tweet = new Tweet(12345, "Yoyo this is super-mega tweet", System.currentTimeMillis(), 100, null, "source", null, null);
      bolt.execute(new Tuple() {
        @Override
        public GlobalStreamId getSourceGlobalStreamid() {
          return null;
        }

        @Override
        public String getSourceComponent() {
          return null;
        }

        @Override
        public int getSourceTask() {
          return 0;
        }

        @Override
        public String getSourceStreamId() {
          return null;
        }

        @Override
        public MessageId getMessageId() {
          return null;
        }

        @Override
        public int size() {
          return 0;
        }

        @Override
        public boolean contains(String field) {
          return false;
        }

        @Override
        public Fields getFields() {
          return null;
        }

        @Override
        public int fieldIndex(String field) {
          return 0;
        }

        @Override
        public List<Object> select(Fields selector) {
          return null;
        }

        @Override
        public Object getValue(int i) {
          return tweet;
        }

        @Override
        public String getString(int i) {
          return null;
        }

        @Override
        public Integer getInteger(int i) {
          return null;
        }

        @Override
        public Long getLong(int i) {
          return null;
        }

        @Override
        public Boolean getBoolean(int i) {
          return null;
        }

        @Override
        public Short getShort(int i) {
          return null;
        }

        @Override
        public Byte getByte(int i) {
          return null;
        }

        @Override
        public Double getDouble(int i) {
          return null;
        }

        @Override
        public Float getFloat(int i) {
          return null;
        }

        @Override
        public byte[] getBinary(int i) {
          return new byte[0];
        }

        @Override
        public Object getValueByField(String field) {
          return null;
        }

        @Override
        public String getStringByField(String field) {
          return null;
        }

        @Override
        public Integer getIntegerByField(String field) {
          return null;
        }

        @Override
        public Long getLongByField(String field) {
          return null;
        }

        @Override
        public Boolean getBooleanByField(String field) {
          return null;
        }

        @Override
        public Short getShortByField(String field) {
          return null;
        }

        @Override
        public Byte getByteByField(String field) {
          return null;
        }

        @Override
        public Double getDoubleByField(String field) {
          return null;
        }

        @Override
        public Float getFloatByField(String field) {
          return null;
        }

        @Override
        public byte[] getBinaryByField(String field) {
          return new byte[0];
        }

        @Override
        public List<Object> getValues() {
          return null;
        }
      }, null);
    } finally {
      bolt.cleanup();
    }
  }

}
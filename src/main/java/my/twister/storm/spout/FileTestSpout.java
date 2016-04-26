package my.twister.storm.spout;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.util.TokenBuffer;
import com.google.common.util.concurrent.RateLimiter;
import my.twister.utils.LogAware;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

/**
 * Created by kkulagin on 4/11/2016.
 */
public class FileTestSpout extends BaseRichSpout implements LogAware {

  public static final String TWEETS_FILE_LOCATION = "tw.file.location";
  public static final String TWEETS_RATE_LIMIT = "tw.rate.limit";
  public static final String TWEETS_FILE_COMPRESSED = "tw.file.compressed";

  private transient InputStream iStream;
  private transient SpoutOutputCollector collector;
  private transient RateLimiter rateLimiter;
  private transient JsonGenerator generator;
  private transient JsonParser jp;
  private transient ByteArrayOutputStream outputStream;
  private transient AtomicLong ids;
  private boolean compressed;
  private String dataFileLocation;
  private JsonFactory jsonFactory;

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("wholeTweet"));
  }

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    this.collector = collector;
    try {
      ids = new AtomicLong(0);
      long rateLimit = (long) conf.get(TWEETS_RATE_LIMIT);
      rateLimiter = RateLimiter.create(rateLimit);

      jsonFactory = new JsonFactory();

      outputStream = new ByteArrayOutputStream(16384);
      generator = jsonFactory.createGenerator(outputStream, JsonEncoding.UTF8);

      dataFileLocation = (String) conf.get(TWEETS_FILE_LOCATION);
      compressed = Optional.ofNullable((Boolean) conf.get(TWEETS_FILE_COMPRESSED)).map(Boolean::booleanValue).orElse(false);

      init();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void init() throws IOException {
//    closeIt(iStream);
    closeIt(jp);
    if (compressed) {
      iStream = new GZIPInputStream(Files.newInputStream(Paths.get(dataFileLocation)));
    } else {
      iStream = Files.newInputStream(Paths.get(dataFileLocation));
    }
    jp = jsonFactory.createParser(iStream);
  }

  @Override
  public void nextTuple() {
    rateLimiter.acquire();
    try {
      TokenBuffer buffer = new TokenBuffer(jp);
      readTweet(jp, buffer);
      buffer.serialize(generator);
      generator.flush();
      collector.emit(new Values(outputStream.toByteArray()), ids.getAndIncrement());
      if (ids.get() % 100_000 == 0) {
        log().info("Emitted " + ids.get());
      }
    } catch (IOException e) {
      try {
        init();
      } catch (IOException e1) {
        throw new RuntimeException(e1);
      }
    } catch (Exception e) {
      try {
        log().error("Error emitting tweet", e);
        StringWriter error = new StringWriter();
        e.printStackTrace(new PrintWriter(error));
        collector.emit("err", new Values("{ Error : " + error.toString()));
      } catch (Exception e1) {
        e1.printStackTrace();
        log().error("Error sending error", e1);
      }

    } finally {
      outputStream.reset();
    }
  }


  private static void readTweet(JsonParser jp, TokenBuffer buffer) throws IOException {
    JsonToken token = jp.nextToken();
    if (token != JsonToken.START_OBJECT) {
      throw new IOException("Expected data to start with an Object");
    }
    buffer.copyCurrentEvent(jp);
    int count = 1;
    while (count > 0) {
      token = jp.nextToken();
      if (token == JsonToken.FIELD_NAME && "timestamp_ms".equals(jp.getCurrentName())) {
        buffer.writeFieldName(jp.getCurrentName());
        jp.nextToken();
        buffer.writeNumber(System.currentTimeMillis());
      } else if (token == JsonToken.END_OBJECT) {
        count--;
        buffer.copyCurrentEvent(jp);
      } else if (token == JsonToken.START_OBJECT) {
        count++;
        buffer.copyCurrentEvent(jp);
      } else {
        buffer.copyCurrentEvent(jp);
      }
    }
  }

  @Override
  public void close() {
    super.close();
    closeIt(generator);
    closeIt(jp);
    closeIt(iStream);
  }

  private void closeIt(Closeable closeable) {
    Optional.ofNullable(closeable).ifPresent((c) -> {
      try {
        c.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    });
  }

}

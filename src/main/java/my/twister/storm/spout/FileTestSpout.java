package my.twister.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.util.TokenBuffer;
import com.google.common.util.concurrent.AtomicLongMap;
import com.google.common.util.concurrent.RateLimiter;
import my.twister.utils.LogAware;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by kkulagin on 4/11/2016.
 */
public class FileTestSpout extends BaseRichSpout implements LogAware {

  public static final String TWEETS_FILE_LOCATION = "tw.file.location";
  public static final String RATE_LIMIT = "tw.rate.limit";

  private transient BufferedReader reader;
  private transient SpoutOutputCollector collector;
  private transient RateLimiter rateLimiter;
  private transient TokenBuffer buffer;
  private transient JsonGenerator generator;
  private transient JsonParser jp;
  private transient ByteArrayOutputStream outputStream;
  private transient AtomicLong ids;

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("wholeTweet"));
  }

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    this.collector = collector;
    try {
      ids = new AtomicLong(0);
      long rateLimit = (long) conf.get(RATE_LIMIT);
      rateLimiter = RateLimiter.create(rateLimit);
      String dataFileLocation = (String) conf.get(TWEETS_FILE_LOCATION);
      JsonFactory jsonF = new JsonFactory();
      reader = Files.newBufferedReader(Paths.get(dataFileLocation));
      jp = jsonF.createParser(reader);
      buffer = new TokenBuffer(jp);
      outputStream = new ByteArrayOutputStream(4096);
      generator = jsonF.createGenerator(outputStream, JsonEncoding.UTF8);

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void nextTuple() {
    rateLimiter.acquire();
    try {
      while (reader.ready()) {
        readTweet(jp, buffer);
        buffer.serialize(generator);
        generator.flush();
        collector.emit(new Values(outputStream.toByteArray()), ids.getAndIncrement());
//        collector.emit(new Values(outputStream.toString(StandardCharsets.UTF_8.name())), ids.getAndIncrement());
      }
    } catch (IOException e) {
      log().error("Error emitting tweet", e);
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
      if (token == JsonToken.END_OBJECT) {
        count--;
      } else if (token == JsonToken.START_OBJECT) {
        count++;
      }

      if (token == JsonToken.FIELD_NAME && "timestamp_ms".equals(jp.getCurrentName())) {
        buffer.writeFieldName(jp.getCurrentName());
        buffer.writeNumber(System.currentTimeMillis());
        jp.nextToken();
      } else {
        buffer.copyCurrentEvent(jp);
      }
    }
  }

  @Override
  public void close() {
    super.close();
    closeIt(reader);
    closeIt(generator);
    closeIt(buffer);
    closeIt(jp);
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

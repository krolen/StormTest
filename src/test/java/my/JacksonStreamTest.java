package my;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.util.TokenBuffer;
import com.google.common.io.CharStreams;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Created by kkulagin on 4/11/2016.
 */
public class JacksonStreamTest {

  public static void main(String[] args) throws IOException {
    JsonFactory jsonF = new JsonFactory();

    try (BufferedReader reader =
             Files.newBufferedReader(Paths.get("C:\\Projects\\Twister\\StormTest\\target\\classes\\realSampleTweet2.json"));
         JsonParser jp = jsonF.createParser(reader);
         TokenBuffer buffer = new TokenBuffer(jp);
         ByteArrayOutputStream outputStream = new ByteArrayOutputStream(4096);
         JsonGenerator generator = jsonF.createGenerator(outputStream, JsonEncoding.UTF8);
    ){;



      while (reader.ready()) {
        readTweet(jp, buffer);
        buffer.serialize(generator);
        generator.flush();
        System.out.println(outputStream.toString("UTF-8"));
        outputStream.reset();
      }

    } catch (IOException e) {
      e.printStackTrace();
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

      if(token == JsonToken.FIELD_NAME && "timestamp_ms".equals(jp.getCurrentName())) {
        buffer.writeFieldName(jp.getCurrentName());
        buffer.writeNumber(111111111111111111L);
        jp.nextToken();
      } else {
        buffer.copyCurrentEvent(jp);
      }
    }

  }
}

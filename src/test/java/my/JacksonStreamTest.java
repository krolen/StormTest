package my;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.util.TokenBuffer;
import com.google.common.io.CharStreams;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.IntStream;
import java.util.zip.GZIPInputStream;

/**
 * Created by kkulagin on 4/11/2016.
 */
public class JacksonStreamTest {

  public static void main(String[] args) throws IOException {
    JsonFactory jsonF = new JsonFactory();

    try (

        InputStream inputStream = Files.newInputStream(Paths.get("C:\\data\\twitter\\firehose_1460399483783.txt.gz"));
//        BufferedReader reader =
//             Files.newBufferedReader(Paths.get("C:\\Projects\\Twister\\StormTest\\target\\classes\\realSampleTweet2.json"));
        InputStream str = new GZIPInputStream(inputStream);
         JsonParser jp = jsonF.createParser(str);
//         TokenBuffer buffer = new TokenBuffer(jp);
         ByteArrayOutputStream outputStream = new ByteArrayOutputStream(4096);

         JsonGenerator generator = jsonF.createGenerator(outputStream, JsonEncoding.UTF8);
    ){;

      int count = 0;
      while (str.available() > 0 && count++ < 5) {
        TokenBuffer buffer = new TokenBuffer(jp);
        outputStream.reset();
        readTweet(jp, buffer);
        buffer.serialize(generator);
        generator.flush();
        System.out.println(outputStream.toString("UTF-8"));
        outputStream.flush();
        System.out.println("------------");
        System.out.println("------------");
        System.out.println("------------");
        System.out.flush();
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
        buffer.writeNumber(System.currentTimeMillis());
        jp.nextToken();
      } else {
        buffer.copyCurrentEvent(jp);
      }
    }

  }
}

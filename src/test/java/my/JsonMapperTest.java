package my;

import com.fasterxml.jackson.databind.ObjectMapper;
import my.twitter.beans.Tweet;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by kkulagin on 5/15/2015.
 */
public class JsonMapperTest {

  private ObjectMapper mapper = new ObjectMapper();

  @Test
  public void testParse() throws IOException {
    byte[] bytes = getBytes();
    Tweet tweet = mapper.readValue(bytes, Tweet.class);
    mapper.writeValue(System.out, tweet);
    System.out.println("yo");
  }

  private byte[] getBytes() throws IOException {
    String dir = System.getProperty("user.dir");
    String input = "KafkaTweet.json";
    Path path = Paths.get(dir + File.separator + input);
    return Files.readAllBytes(path);
  }

}

package my.twister.storm.spout;


import org.apache.storm.spout.RawScheme;
import org.apache.storm.tuple.Fields;

/**
 * Created by kkulagin on 5/13/2015.
 */
public class TwitterSchema extends RawScheme {

  @Override
  public Fields getOutputFields() {
    return new Fields("WholeTweet");
  }
}

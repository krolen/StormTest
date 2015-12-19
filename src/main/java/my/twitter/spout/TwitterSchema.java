package my.twitter.spout;

import backtype.storm.spout.RawScheme;
import backtype.storm.tuple.Fields;

/**
 * Created by kkulagin on 5/13/2015.
 */
public class TwitterSchema extends RawScheme {

  @Override
  public Fields getOutputFields() {
    return new Fields("WholeTweet");
  }
}

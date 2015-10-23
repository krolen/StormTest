package my.twitter.spout;

import twitter4j.*;
import twitter4j.conf.ConfigurationContext;

/**
 * Created by kkulagin on 10/23/2015.
 */
public class SampleTwitterSpout {



  protected void setup() {
    StatusListener listener = new StatusListener(){
      public void onStatus(Status status) {
        System.out.println(status.getUser().getName() + " : " + status.getText());
      }

      @Override
      public void onScrubGeo(long userId, long upToStatusId) {

      }

      @Override
      public void onStallWarning(StallWarning warning) {

      }

      public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
      public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}
      public void onException(Exception ex) {
        ex.printStackTrace();
      }
    };
    TwitterStreamFactory factory = new TwitterStreamFactory();
    TwitterStream twitterStream = factory.getInstance();
    twitterStream.addListener(listener);
    // sample() method internally creates a thread which manipulates TwitterStream and calls these adequate listener methods continuously.
    twitterStream.sample();
  }

  public static void main(String[] args) {
    new SampleTwitterSpout().setup();
  }
}

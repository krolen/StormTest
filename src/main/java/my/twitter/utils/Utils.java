package my.twitter.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * Created by kkulagin on 5/15/2015.
 */
public class Utils {

  private static final String twitter = "EEE MMM dd HH:mm:ss ZZZZ yyyy";
  private static final SimpleDateFormat sf = new SimpleDateFormat(twitter, Locale.ENGLISH);

  public static Date getTwitterDate(String date) throws ParseException {
    sf.setLenient(true);
    return sf.parse(date);
  }

  public static void main(String[] args) throws ParseException {
    String date = "Mon Dec 03 16:17:46 +0000 2012";
    Date twitterDate = getTwitterDate(date);
    System.out.println(twitterDate);
  }
}

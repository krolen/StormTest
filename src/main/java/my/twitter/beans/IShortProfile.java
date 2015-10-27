package my.twitter.beans;

/**
 * @author kkulagin
 * @since 26.10.2015
 */
public interface IShortProfile {
  int getAuthority();

  byte isVerified();

  int getFollowersCount();

  int getFriendsCount();

  int getPostCount();

}

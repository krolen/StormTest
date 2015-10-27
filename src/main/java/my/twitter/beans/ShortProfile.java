package my.twitter.beans;

/**
 * Created by kkulagin on 5/15/2015.
 */
public class ShortProfile implements IShortProfile {
  private byte verified;
  private byte authority;
  private int followersCount;
  private int friendsCount;
  private int postCount;

  public ShortProfile(Profile profile) {
    verified = (byte) (profile.isVerified() ? 1 : 0);
    authority = (byte) profile.getAuthority();
    followersCount = profile.getFollowersCount();
    friendsCount = profile.getFriendsCount();
    postCount = profile.getPostCount();
  }

  public ShortProfile() {
  }

  @Override
  public int getAuthority() {
    return authority;
  }

  @Override
  public byte isVerified() {
    return verified;
  }

  @Override
  public int getFollowersCount() {
    return followersCount;
  }

  @Override
  public int getFriendsCount() {
    return friendsCount;
  }

  @Override
  public int getPostCount() {
    return postCount;
  }

  public void setVerified(byte verified) {
    this.verified = verified;
  }

  public void setAuthority(byte authority) {
    this.authority = authority;
  }

  public void setFollowersCount(int followersCount) {
    this.followersCount = followersCount;
  }

  public void setFriendsCount(int friendsCount) {
    this.friendsCount = friendsCount;
  }

  public void setPostCount(int postCount) {
    this.postCount = postCount;
  }

  @Override
  public String toString() {
    return "SaveProfile{" +
      "verified=" + verified +
      ", authority=" + authority +
      ", followersCount=" + followersCount +
      ", friendsCount=" + friendsCount +
      ", postCount=" + postCount +
      '}';
  }
}

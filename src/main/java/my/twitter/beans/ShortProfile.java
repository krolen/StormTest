package my.twitter.beans;

/**
 * Created by kkulagin on 5/15/2015.
 */
public class ShortProfile implements IShortProfile {
  private boolean verified;
  private byte authority;
  private int followersCount;
  private int friendsCount;
  private int postCount;
  private long modifiedTime;

  public ShortProfile(Profile profile) {
    verified = profile.isVerified();
    authority = (byte) profile.getAuthority();
    followersCount = profile.getFollowersCount();
    friendsCount = profile.getFriendsCount();
    postCount = profile.getPostCount();
  }

  public ShortProfile() {
  }

  @Override
  public byte getAuthority() {
    return authority;
  }

  @Override
  public boolean isVerified() {
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

  @Override
  public long getModifiedTime() {
    return modifiedTime;
  }

  @Override
  public void setVerified(boolean verified) {
    this.verified = verified;
  }

  @Override
  public void setAuthority(byte authority) {
    this.authority = authority;
  }

  @Override
  public void setFollowersCount(int followersCount) {
    this.followersCount = followersCount;
  }

  @Override
  public void setFriendsCount(int friendsCount) {
    this.friendsCount = friendsCount;
  }

  @Override
  public void setPostCount(int postCount) {
    this.postCount = postCount;
  }

  @Override
  public void setModifiedTime(long time) {
    this.modifiedTime = time;
  }

  @Override
  public String toString() {
    return "ShortProfile{" +
        "verified=" + verified +
        ", authority=" + authority +
        ", followersCount=" + followersCount +
        ", friendsCount=" + friendsCount +
        ", postCount=" + postCount +
        ", modifiedTime=" + modifiedTime +
        '}';
  }
}

package my.twitter.beans;

import com.fasterxml.jackson.annotation.*;
import my.twitter.utils.Utils;

import java.text.ParseException;

/**
 * Created by kkulagin on 5/15/2015.
 */
@JsonIgnoreProperties(ignoreUnknown = true)

public class Profile {
  public long id;
  public String id_str;
  public String name;
  public String screenName;
  public String description;
  public boolean verified;
  private String pictureUrl;
  private String language;
  private int followersCount;
  private int friendsCount;
  private int postCount;
  private String created;

  @JsonSetter
  public void setCreated_at(String value) throws ParseException {
    created = value;
  }

  @JsonGetter
  public String getCreated() {
    return created;
  }

  @JsonGetter(value = "postCount")
  public int getPostCount() {
    return postCount;
  }

  @JsonSetter(value = "statuses_count")
  public void setPostCount(int postCount) {
    this.postCount = postCount;
  }

  @JsonGetter(value = "friendsCount")
  public int getFriendsCount() {
    return friendsCount;
  }

  @JsonSetter(value = "friends_count")
  public void setFriendsCount(int friendsCount) {
    this.friendsCount = friendsCount;
  }

  @JsonGetter(value = "followersCount")
  public int getFollowersCount() {
    return followersCount;
  }

  @JsonSetter(value = "followers_count")
  public void setFollowersCount(int followersCount) {
    this.followersCount = followersCount;
  }

  @JsonGetter(value = "pictureUrl")
  public String getPictureUrl() {
    return pictureUrl;
  }

  @JsonSetter(value = "profile_image_url")
  public void setPictureUrl(String pictureUrl) {
    this.pictureUrl = pictureUrl;
  }

  @JsonSetter(value = "language")
  public String getLanguage() {
    return language;
  }

  @JsonSetter(value = "lang")
  public void setLanguage(String language) {
    this.language = language;
  }
}

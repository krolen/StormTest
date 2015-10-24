package my.twitter.beans;

import com.fasterxml.jackson.annotation.*;

/**
 * Created by kkulagin on 5/15/2015.
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Profile {
  @JsonProperty("id")
  private long id;
  private String name;
  @JsonProperty("screen_name")
  private String screenName;
  private String description;
  private boolean verified;
  @JsonProperty("profile_image_url")
  private String pictureUrl;
  @JsonProperty("lang")
  private String language;
  @JsonProperty("followers_count")
  private int followersCount;
  @JsonProperty("friends_count")
  private int friendsCount;
  @JsonProperty("statuses_count")
  private int postCount;
  @JsonProperty("created_at")
  private String created;

  public long getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public String getScreenName() {
    return screenName;
  }

  public String getDescription() {
    return description;
  }

  public boolean isVerified() {
    return verified;
  }

  public String getPictureUrl() {
    return pictureUrl;
  }

  public String getLanguage() {
    return language;
  }

  public int getFollowersCount() {
    return followersCount;
  }

  public int getFriendsCount() {
    return friendsCount;
  }

  public int getPostCount() {
    return postCount;
  }

  public String getCreated() {
    return created;
  }

  @Override
  public String toString() {
    return "Profile{" +
      "id=" + id +
      ", name='" + name + '\'' +
      ", screenName='" + screenName + '\'' +
      ", description='" + description + '\'' +
      ", verified=" + verified +
      ", pictureUrl='" + pictureUrl + '\'' +
      ", language='" + language + '\'' +
      ", followersCount=" + followersCount +
      ", friendsCount=" + friendsCount +
      ", postCount=" + postCount +
      ", created='" + created + '\'' +
      '}';
  }
}

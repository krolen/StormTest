package my.twitter.beans;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by kkulagin on 5/15/2015.
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Profile {
  private boolean verified;
  private int authority;
  @JsonProperty("followers_count")
  private int followersCount;
  @JsonProperty("friends_count")
  private int friendsCount;
  @JsonProperty("statuses_count")
  private int postCount;

  @JsonProperty("id")
  private long id;
  private String name;
  @JsonProperty("screen_name")
  private String screenName;
  private String description;
  @JsonProperty("created_at")
  private String created;
  @JsonProperty("profile_image_url")
  private String pictureUrl;
  @JsonProperty("lang")
  private String language;

  @JsonProperty("location")
  private String location;

  public void setAuthority(int authority) {
    this.authority = authority;
  }

  public int getAuthority() {
    return authority;
  }

  public long getId() {
    return id;
  }

  public String getLocation() {
    return location;
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

  public String getPictureUrl() {
    return pictureUrl;
  }

  public String getLanguage() {
    return language;
  }

  public boolean isVerified() {
    return verified;
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
      ", location='" + location + '\'' +
      ", followersCount=" + followersCount +
      ", friendsCount=" + friendsCount +
      ", postCount=" + postCount +
      ", created='" + created + '\'' +
      ", authority=" + authority +
      '}';
  }
}

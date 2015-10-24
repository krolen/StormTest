package my.twitter.beans;

import com.fasterxml.jackson.annotation.*;

/**
 * Created by kkulagin on 5/15/2015.
 */

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Tweet {
  @JsonProperty("id")
  private long id;
  @JsonProperty("text")
  private String contents;
  @JsonProperty("timestamp_ms")
  private long createDate;
  private long authorId;
  @JsonProperty("lang")
  private String lang;
  @JsonIgnore
  private Profile user;

  public long getId() {
    return id;
  }

  public String getContents() {
    return contents;
  }

  public long getCreateDate() {
    return createDate;
  }

  public long getAuthorId() {
    return authorId;
  }

  public String getLang() {
    return lang;
  }

  @JsonSetter
  public void setUser(Profile user) {
    this.user = user;
    authorId = user.getId();
  }

  @JsonIgnore
  public Profile getUser() {
    return user;
  }

  @Override
  public String toString() {
    return "Tweet{" +
      "id=" + id +
      ", contents='" + contents + '\'' +
      ", createDate=" + createDate +
      ", authorId=" + authorId +
      ", lang='" + lang + '\'' +
      ", user=" + user +
      '}';
  }
}

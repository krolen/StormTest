package my.twitter.beans;

import com.fasterxml.jackson.annotation.*;

/**
 * Created by kkulagin on 5/15/2015.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Tweet {
  public long id;
  private String contents;
  private long createDate;
  public int authorId;
  public String lang;
  private Profile user;

  @JsonGetter("contents")
  public String getContents() {
    return contents;
  }

  @JsonSetter("text")
  public void setContents(String contents) {
    this.contents = contents;
  }

  @JsonGetter("createDate")
  public long getCreateDate() {
    return createDate;
  }

  @JsonSetter("timestamp_ms")
  public void setCreateDate(long createDate) {
    this.createDate = createDate;
  }

  @JsonSetter
  public void setUser(Profile user) {
    this.user = user;
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

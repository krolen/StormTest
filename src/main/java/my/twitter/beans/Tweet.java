package my.twitter.beans;

import com.fasterxml.jackson.annotation.*;

import java.util.Arrays;

/**
 * Created by kkulagin on 5/15/2015.
 */

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Tweet  {
  @JsonProperty("id")
  private long id;
  @JsonProperty("text")
  private String contents;
  @JsonProperty("timestamp_ms")
  private long createDate;
  private long authorId;
  @JsonProperty("lang")
  private String lang;
  @JsonProperty("source")
  private String source;
  private long[] mentions;


  public long[] getMentions() {
    return mentions;
  }

  public void setMentions(long[] mentions) {
    this.mentions = mentions;
  }

  @JsonIgnore // skipped in stream
  private Profile user;

  public String getSource() {
    return source;
  }

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

  public void prepareForSerialization() {
    user = null; //to skip kryo serialization
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
        ", source='" + source + '\'' +
        ", mentions=" + Arrays.toString(mentions) +
        ", user=" + user +
        '}';
  }
}

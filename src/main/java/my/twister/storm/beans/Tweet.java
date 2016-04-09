package my.twister.storm.beans;

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
  private Long retweetedTweetUserId = 0L;

  @JsonIgnore // skipped in stream
  private Profile user;

  public Tweet() {
  }

  public Tweet(long id, String contents, long createDate, long authorId, String lang, String source, long[] mentions, Profile user) {
    this.id = id;
    this.contents = contents;
    this.createDate = createDate;
    this.authorId = authorId;
    this.lang = lang;
    this.source = source;
    this.mentions = mentions;
    this.user = user;
  }

  public long[] getMentions() {
    return mentions;
  }

  public void setMentions(long[] mentions) {
    this.mentions = mentions;
  }


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

  public Long getRetweetedTweetUserId() {
    return retweetedTweetUserId;
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

  @JsonSetter(value = "retweeted_status")
  public void setRetweetedStatus(RetweetedStatus retweetedStatus) {
    retweetedTweetUserId = retweetedStatus.getUser().getId();
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

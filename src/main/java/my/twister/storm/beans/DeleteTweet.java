package my.twister.storm.beans;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;

/**
 * @author kkulagin
 * @since 24.10.2015
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DeleteTweet {
  private long timestamp;
  private long id;
  private long userId;

  public long getTimestamp() {
    return timestamp;
  }

  public long getId() {
    return id;
  }

  public long getUserId() {
    return userId;
  }

  @JsonSetter("timestamp_ms")
  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  @JsonSetter("status")
  public void setStatus(Status status) {
    id = status.id;
    userId = status.userId;
  }


  @Override
  public String toString() {
    return "DeleteTweet{" +
      "timestamp=" + timestamp +
      ", id=" + id +
      ", userId=" + userId +
      '}';
  }

  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
  @JsonIgnoreProperties(ignoreUnknown = true)
  static class Status {
    private long id;
    @JsonProperty("user_id")
    private long userId;
  }
}

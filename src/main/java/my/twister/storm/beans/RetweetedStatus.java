package my.twister.storm.beans;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by kkulagin on 4/9/2016.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class RetweetedStatus {
  @JsonProperty
  private Profile user;

  public Profile getUser() {
    return user;
  }

  public void setUser(Profile user) {
    this.user = user;
  }
}

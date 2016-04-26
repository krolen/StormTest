package my.twister.storm.bolts.profile;

import my.twister.chronicle.ChronicleDataService;
import my.twister.storm.beans.Profile;
import my.twister.utils.LogAware;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.values.Values;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by kkulagin on 10/26/2015.
 */
public class AmendProfileBolt extends BaseBasicBolt implements LogAware {

  private transient StringBuilder nameBuffer;
  private transient int count;
  private transient LongValue profileIdValue;
  private transient LongValue timeValue;
  private transient ChronicleDataService chronicleDataService;
  private transient ChronicleDataService.MapReference<LongValue, LongValue> id2TimeMapReference;
  private transient ChronicleDataService.MapReference<CharSequence, LongValue> name2IdMapReference;

  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    System.out.println("Amend setup start");
    chronicleDataService = ChronicleDataService.getInstance();
    id2TimeMapReference = chronicleDataService.connectId2TimeMap();
    name2IdMapReference = chronicleDataService.connectName2IdMap();
    System.out.println("Maps done");
    nameBuffer = new StringBuilder();
    profileIdValue = Values.newHeapInstance(LongValue.class);
    count = 0;
    timeValue = Values.newHeapInstance(LongValue.class);
    System.out.println("Amend setup done");
  }

  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {
    Profile profile = (Profile) input.getValue(0);
    nameBuffer.setLength(0);
    profileIdValue.setValue(profile.getId());

    String screenName = profile.getScreenName();
    if (screenName != null) {
      for (count = 0; count < screenName.length(); count++) {
        nameBuffer.append(Character.toLowerCase(screenName.charAt(count)));
      }
      name2IdMapReference.map().put(nameBuffer, profileIdValue);
    }

    timeValue.setValue(System.currentTimeMillis());
    id2TimeMapReference.map().put(profileIdValue, timeValue);

    profile.setAuthority(calculateAuthority(profile));
    collector.emit("storeProfile", new org.apache.storm.tuple.Values(profile));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declareStream("storeProfile", new Fields("profile"));
  }

  @Override
  public void cleanup() {
    chronicleDataService.release(id2TimeMapReference);
    chronicleDataService.release(name2IdMapReference);
    super.cleanup();
  }

  private static int calculateAuthority(Profile profile) {
    int numFollowers = profile.getFollowersCount();
    int numFollowing = profile.getFriendsCount();
    int numUpdates = Math.abs(profile.getPostCount());
    int fC = numFollowers + Math.max(0, numFollowers - numFollowing);
    fC = Math.abs(fC) / 4;
    double maxUpdatesAuth = Math.min(2.d, (Math.log(1 + numUpdates) / Math.log(100)));
    double maxFollowerAuth = Math.log(1 + fC) / Math.log(3.4);
    maxFollowerAuth = Math.max(0.d, maxFollowerAuth - 0.5d);
    int auth = (int) (maxFollowerAuth + maxUpdatesAuth);
    return Math.min(auth, 10);
  }

}

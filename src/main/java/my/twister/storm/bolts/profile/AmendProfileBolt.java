package my.twister.storm.bolts.profile;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import my.twister.chronicle.ChronicleDataService;
import my.twister.entities.IShortProfile;
import my.twister.storm.beans.Profile;
import my.twister.storm.bolts.profile.chronicle.StormCDSSingletonWrapper;
import my.twister.utils.LogAware;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.values.Values;

import java.util.Map;

/**
 * Created by kkulagin on 10/26/2015.
 */
public class AmendProfileBolt extends BaseBasicBolt implements LogAware {

  private transient ChronicleMap<CharSequence, LongValue> name2IdMap;
  private transient ChronicleMap<LongValue, LongValue> id2TimeMap;
  private transient ChronicleMap<Long, IShortProfile> id2ProfileMap;
  private transient StringBuilder nameBuffer;
  private transient int count;
  private transient LongValue profileIdValue;
  private transient LongValue timeValue;
  private ChronicleDataService chronicleDataService;

  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    System.out.println("Amend setup start");
    chronicleDataService = StormCDSSingletonWrapper.getInstance();
    chronicleDataService.connect(3);

    name2IdMap = chronicleDataService.getName2IdMap();
    id2TimeMap = chronicleDataService.getId2TimeMap();
    System.out.println("Maps done");
    nameBuffer = new StringBuilder();
    profileIdValue = Values.newHeapInstance(LongValue.class);
    count = 0;
    timeValue = Values.newHeapInstance(LongValue.class);
//    fileLocation = (String) stormConf.get("profile.id.to.profile.file");
//    file = new File(fileLocation);
//    ChronicleMapBuilder<Long, IShortProfile> builder =
//      ChronicleMapBuilder.of(Long.class, IShortProfile.class).
//        constantValueSizeBySample(new ShortProfile()).
//        entries(400_000_000);
//    try {
//      id2ProfileMap = builder.createPersistedTo(file);
//    } catch (IOException e) {
//      // fail fast
//      throw new RuntimeException(e);
//    }
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
      name2IdMap.put(nameBuffer, profileIdValue);
    }

    timeValue.setValue(System.currentTimeMillis());
    id2TimeMap.put(profileIdValue, timeValue);

    profile.setAuthority(calculateAuthority(profile));
    collector.emit("storeProfile", new backtype.storm.tuple.Values(profile));

//    IShortProfile using = Values.newHeapInstance(IShortProfile.class);
//    using.setAuthority((byte)profile.getAuthority());
//    using.setFollowersCount(profile.getFollowersCount());
//    using.setFriendsCount(profile.getFriendsCount());
//    using.setPostCount(profile.getPostCount());
//    using.setVerified(profile.isVerified());
//    using.setModifiedTime(profile.getModifiedTime());
//
//    id2ProfileMap.put(profile.getId(), using);
//    log().info("id2ProfileMap size is " + id2ProfileMap.longSize());
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declareStream("storeProfile", new Fields("profile"));
  }

  @Override
  public void cleanup() {
    chronicleDataService.close();
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

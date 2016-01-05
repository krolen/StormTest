package my.twitter.bolts.profile;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.google.common.collect.Maps;
import my.twitter.beans.IShortProfile;
import my.twitter.beans.Profile;
import my.twitter.bolts.profile.chronicle.ChronicleDataService;
import my.twitter.utils.LogAware;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.values.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by kkulagin on 10/26/2015.
 */
public class AmendProfileBolt extends BaseBasicBolt implements LogAware {

  private transient ChronicleMap<CharSequence, LongValue> name2IdMap;
  private transient ChronicleMap<Long, LongValue> id2TimeMap;
  private transient ChronicleMap<Long, IShortProfile> id2ProfileMap;
  private transient StringBuilder nameBuffer;
  private transient int count;
  private transient LongValue profileIdValue;
  private transient LongValue timeValue;

  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    name2IdMap = ChronicleDataService.getInstance(stormConf).getName2IdMap();
    id2TimeMap = ChronicleDataService.getInstance(stormConf).getId2TimeMap();
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
  }

  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {
    Profile profile = (Profile) input.getValue(0);
    nameBuffer.setLength(0);
    for (count = 0; count < profile.getScreenName().length(); count++) {
      nameBuffer.append(Character.toLowerCase(profile.getScreenName().charAt(count)));
    }
    profileIdValue.setValue(profile.getId());
    name2IdMap.put(nameBuffer, profileIdValue);

    timeValue.setValue(System.currentTimeMillis());
    id2TimeMap.put(profile.getId(), timeValue);

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
    name2IdMap.close();
    id2TimeMap.close();
//    id2ProfileMap.close();
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

  private void testSaveProfile(Profile profile) {
    IShortProfile using = Values.newHeapInstance(IShortProfile.class);
    using.setAuthority((byte) profile.getAuthority());
    using.setFollowersCount(profile.getFollowersCount());
    using.setFriendsCount(profile.getFriendsCount());
    using.setPostCount(profile.getPostCount());
    using.setVerified(profile.isVerified());
    id2ProfileMap.put(profile.getId() + System.currentTimeMillis(), using);
  }

  public static void main(String[] args) {
    HashMap<Object, Object> map = Maps.newHashMap();
    map.put("profile.name.to.id.file", "c:/data/profile/name2id");
    map.put("profile.id.to.profile.file", "c:/data/profile/id2profile");
    AmendProfileBolt amendProfileBolt = new AmendProfileBolt();
    amendProfileBolt.prepare(map, null);

    Profile profile = new Profile();
    profile.setAuthority(12);
    amendProfileBolt.testSaveProfile(profile);

    amendProfileBolt.cleanup();

    amendProfileBolt.prepare(map, null);
//    System.out.println(amendProfileBolt.id2ProfileMap.longSize());
//    Set<Map.Entry<Long, IShortProfile>> entries = amendProfileBolt.id2ProfileMap.entrySet();
//    for (Map.Entry<Long, IShortProfile> entry : entries) {
//      System.out.println("entry = " + entry.getKey() + " : " + entry.getValue());
//    }
    amendProfileBolt.cleanup();
  }
}

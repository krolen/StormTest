package my.twitter.bolts.profile.chronicle;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.google.common.collect.Maps;
import my.twitter.beans.IShortProfile;
import my.twitter.beans.Profile;
import my.twitter.utils.LogAware;
import net.openhft.chronicle.map.*;
import net.openhft.chronicle.values.Values;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created by kkulagin on 10/26/2015.
 */
public class AmendProfileBolt extends BaseRichBolt implements LogAware {

  private OutputCollector collector;
  private ChronicleMap<String, Long> name2IdMap;
  private ChronicleMap<Long, IShortProfile> id2ProfileMap;

  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
    String fileLocation = (String) stormConf.get("profile.name.to.id.file");
    File file = new File(fileLocation);
    ChronicleMapBuilder<String, Long> name2IdMapBuilder =
      ChronicleMapBuilder.of(String.class, Long.class).
        averageKey("this_is_18_charctr").
        entries(1000);
    try {
      name2IdMap = name2IdMapBuilder.createPersistedTo(file);
    } catch (IOException e) {
      // fail fast
      throw new RuntimeException(e);
    }

//    fileLocation = (String) stormConf.get("profile.id.to.profile.file");
//    file = new File(fileLocation);
//    ChronicleMapBuilder<Long, IShortProfile> builder =
//      ChronicleMapBuilder.of(Long.class, IShortProfile.class).
//        constantValueSizeBySample(new ShortProfile()).
//        entries(1000);
//    try {
//      id2ProfileMap = builder.createPersistedTo(file);
//    } catch (IOException e) {
//      // fail fast
//      throw new RuntimeException(e);
//    }
  }

  @Override
  public void execute(Tuple input) {
    Profile profile = (Profile) input.getValue(0);
    name2IdMap.put(profile.getScreenName(), profile.getId());

    profile.setAuthority(calculateAuthority(profile));

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

    collector.ack(input);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

  }

  @Override
  public void cleanup() {
    name2IdMap.close();
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
    using.setAuthority((byte)profile.getAuthority());
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
    amendProfileBolt.prepare(map, null, null);

    Profile profile = new Profile();
    profile.setAuthority(12);
    amendProfileBolt.testSaveProfile(profile);

    amendProfileBolt.cleanup();

    amendProfileBolt.prepare(map, null, null);
//    System.out.println(amendProfileBolt.id2ProfileMap.longSize());
//    Set<Map.Entry<Long, IShortProfile>> entries = amendProfileBolt.id2ProfileMap.entrySet();
//    for (Map.Entry<Long, IShortProfile> entry : entries) {
//      System.out.println("entry = " + entry.getKey() + " : " + entry.getValue());
//    }
    amendProfileBolt.cleanup();
  }
}

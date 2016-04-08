package my.twister.storm.bolts.profile;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
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
 * Created by kkulagin on 5/13/2015.
 */
public class StoreProfileBolt extends BaseBasicBolt implements LogAware {

  private ChronicleDataService chronicleDataService;
  private ChronicleMap<LongValue, IShortProfile> id2ProfileMap;
  private LongValue profileIdValue;
  private IShortProfile profileValue;

  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    System.out.println("StoreProfileBolt setup start");
    chronicleDataService = StormCDSSingletonWrapper.getInstance();
    chronicleDataService.connect(3);
    id2ProfileMap = chronicleDataService.getId2ProfileMap();

    profileIdValue = Values.newHeapInstance(LongValue.class);
    profileValue = Values.newHeapInstance(IShortProfile.class);
    System.out.println("StoreProfileBolt setup done");
  }

  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {
    Profile profile = (Profile) input.getValue(0);

    profileIdValue.setValue(profile.getId());
    profileValue.setAuthority(profile.getAuthority());
    profileValue.setFollowersCount(profile.getFollowersCount());
    profileValue.setFriendsCount(profile.getFriendsCount());
    profileValue.setModifiedTime(profile.getModifiedTime());
    profileValue.setPostCount(profile.getPostCount());
    profileValue.setVerified(profile.isVerified());
    id2ProfileMap.put(profileIdValue, profileValue);

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

  }

}

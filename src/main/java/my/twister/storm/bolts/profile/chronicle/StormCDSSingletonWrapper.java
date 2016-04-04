package my.twister.storm.bolts.profile.chronicle;

import my.twister.chronicle.ChronicleDataService;
import my.twister.entities.IShortProfile;
import my.twister.entities.IShortTweet;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.map.ChronicleMap;

import javax.management.*;
import java.lang.management.ManagementFactory;

/**
 * Created by kkulagin on 4/4/2016.
 */
public class StormCDSSingletonWrapper extends ChronicleDataService {

  private static volatile ChronicleDataService instance;

  public static ChronicleDataService getInstance() {
    if (instance == null) {
      synchronized (StormCDSSingletonWrapper.class) {
        if (instance == null) {
          instance = new StormCDSSingletonWrapper();
        }
      }
    }
    return instance;
  }

  private final ChronicleDataService wrapped;
  private int connections = 0;

  private StormCDSSingletonWrapper () {
    wrapped = ChronicleDataService.getInstance();
  }

  @Override
  public ChronicleMap<CharSequence, LongValue> getName2IdMap() {
    return wrapped.getName2IdMap();
  }

  @Override
  public ChronicleMap<LongValue, IShortProfile> getId2ProfileMap() {
    return wrapped.getId2ProfileMap();
  }

  @Override
  public ChronicleMap<LongValue, LongValue> getId2TimeMap() {
    return wrapped.getId2TimeMap();
  }

  @Override
  public ChronicleMap<LongValue, IShortTweet> getTweetsDataMap(long tweetId) {
    return wrapped.getTweetsDataMap(tweetId);
  }

  @Override
  public synchronized void connect(int i) {
    log().info("Connecting map services, number of open connections " + connections);
    if (connections == 0) {
      wrapped.connect(4);
      try {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        ObjectName name = new ObjectName("my.metric.chronicle:type=MyMetrics");
        MyMetrics mbean = new MyMetrics(this);
        mbs.registerMBean(mbean, name);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    connections++;
    log().info("Number of connections after connect " + connections);
  }

  @Override
  public synchronized void close() {
    log().info("Closing connection to map services, number of open connections " + connections);
    connections--;
    if (connections == 0) {
      wrapped.close();
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      try {
        mbs.unregisterMBean(new ObjectName("my.metric.chronicle:type=MyMetrics"));
      } catch (Exception e) {
        log().error("Cannot unregister bean", e);
      }
    }
    log().info("Number of connections after close " + connections);
  }
}

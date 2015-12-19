package my.twitter.bolts.profile.chronicle;

import my.twitter.beans.IShortProfile;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Map;

/**
 * Created by kkulagin on 12/19/2015.
 */
public abstract class ChronicleDataService {

  public abstract  ChronicleMap<String, Long> getName2IdMap();

  public abstract  ChronicleMap<Long, IShortProfile> getId2ProfileMap();

  private static volatile ChronicleDataService instance;

  public static ChronicleDataService getInstance(Map stormConf) {
    if(stormConf == null && instance == null) {
      throw new IllegalStateException("Not initialized yet");
    }
    if(instance == null) {
      synchronized (ChronicleDataService.class) {
        if(instance == null) {
          DefaultChronicleDataService created = new DefaultChronicleDataService(stormConf);
          created.init();
          instance = created;
        }
      }
    }
    return instance;
  }

  private static class DefaultChronicleDataService extends ChronicleDataService {

    private final ChronicleMap<String, Long> name2IdMap;
    private ChronicleMap<Long, IShortProfile> id2ProfileMap;

    private DefaultChronicleDataService(Map stormConf) {
      String fileLocation = (String) stormConf.get("profile.name.to.id.file");
      File file = new File(fileLocation);
      if (!file.exists()) {
        try {
          file.getParentFile().mkdirs();
          file.createNewFile();
        } catch (IOException e) {
          // fail fast
          throw new RuntimeException(e);
        }
      }
      ChronicleMapBuilder<String, Long> name2IdMapBuilder =
          ChronicleMapBuilder.of(String.class, Long.class).
              averageKey("this_is_18_charctr").
              entries(System.getProperty("os.name").toLowerCase().contains("win")? 1000 : 400_000_000);
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
//        entries(400_000_000);
//    try {
//      id2ProfileMap = builder.createPersistedTo(file);
//    } catch (IOException e) {
//      // fail fast
//      throw new RuntimeException(e);
//    }

    }

    private void init() {
      try {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        ObjectName name = new ObjectName("my.metric.chronicle:type=MyMetrics");
        MyMetrics mbean = new MyMetrics(this);
        mbs.registerMBean(mbean, name);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public ChronicleMap<String, Long> getName2IdMap() {
      return name2IdMap;
    }

    @Override
    public ChronicleMap<Long, IShortProfile> getId2ProfileMap() {
      return id2ProfileMap;
    }
  }
}

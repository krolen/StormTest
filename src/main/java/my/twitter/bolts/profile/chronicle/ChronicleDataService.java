package my.twitter.bolts.profile.chronicle;

import my.twitter.beans.IShortProfile;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.jetbrains.annotations.NotNull;

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

  public abstract  ChronicleMap<Long, Long> getTime2IdMap();

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
    private final ChronicleMap<Long, Long> time2IdMap;
    private ChronicleMap<Long, IShortProfile> id2ProfileMap;

    private DefaultChronicleDataService(Map stormConf) {
      String name2IdFileLocation = (String) stormConf.get("profile.name.to.id.file");
      File name2IdFile = getFile(name2IdFileLocation);
      ChronicleMapBuilder<String, Long> name2IdMapBuilder =
          ChronicleMapBuilder.of(String.class, Long.class).
              averageKey("this_is_18_charctr").
              entries(System.getProperty("os.name").toLowerCase().contains("win")? 1000 : 400_000_000);
      try {
        name2IdMap = name2IdMapBuilder.createPersistedTo(name2IdFile);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      String time2IdFileLocation = (String) stormConf.get("profile.time.to.id.file");
      File time2IdFile = getFile(time2IdFileLocation);
      ChronicleMapBuilder<Long, Long> time2IdMapBuilder =
          ChronicleMapBuilder.of(Long.class, Long.class).
              entries(System.getProperty("os.name").toLowerCase().contains("win")? 1000 : 400_000_000);
      try {
        time2IdMap = time2IdMapBuilder.createPersistedTo(time2IdFile);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

//    name2IdFileLocation = (String) stormConf.get("profile.id.to.profile.name2IdFile");
//    name2IdFile = new File(name2IdFileLocation);
//    ChronicleMapBuilder<Long, IShortProfile> builder =
//      ChronicleMapBuilder.of(Long.class, IShortProfile.class).
//        constantValueSizeBySample(new ShortProfile()).
//        entries(400_000_000);
//    try {
//      id2ProfileMap = builder.createPersistedTo(name2IdFile);
//    } catch (IOException e) {
//      // fail fast
//      throw new RuntimeException(e);
//    }

    }

    @NotNull
    private File getFile(String fileLocation) {
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
      return file;
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
    public ChronicleMap<Long, Long> getTime2IdMap() {
      return time2IdMap;
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

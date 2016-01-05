package my.twitter.bolts.profile.chronicle;

import my.twitter.beans.IShortProfile;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.map.ChronicleMap;
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

  public abstract ChronicleMap<CharSequence, LongValue> getName2IdMap();

  public abstract ChronicleMap<Long, IShortProfile> getId2ProfileMap();

  public abstract ChronicleMap<Long, LongValue> getId2TimeMap();

  private static volatile ChronicleDataService instance;

  public static ChronicleDataService getInstance(Map stormConf) {
    if (stormConf == null && instance == null) {
      throw new IllegalStateException("Not initialized yet");
    }
    if (instance == null) {
      synchronized (ChronicleDataService.class) {
        if (instance == null) {
          DefaultChronicleDataService created = new DefaultChronicleDataService(stormConf);
          created.init();
          instance = created;
        }
      }
    }
    return instance;
  }

  private static class DefaultChronicleDataService extends ChronicleDataService {

    private final ChronicleMap<CharSequence, LongValue> name2IdMap;
    private final ChronicleMap<Long, LongValue> id2TimeMap;
    private ChronicleMap<Long, IShortProfile> id2ProfileMap;

    private DefaultChronicleDataService(Map stormConf) {
      String name2IdFileLocation = (String) stormConf.get("profile.name.to.id.file");
      File name2IdFile = getFile(name2IdFileLocation);
      try {
        name2IdMap = ChronicleMap.of(CharSequence.class, LongValue.class).putReturnsNull(true).
            averageKeySize("this_is_18_charctr".getBytes().length).
            entries(System.getProperty("os.name").toLowerCase().contains("win") ? 10_000 : 400_000_000).
            createPersistedTo(name2IdFile);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      String time2IdFileLocation = (String) stormConf.get("profile.id.to.time.file");
      File time2IdFile = getFile(time2IdFileLocation);
      try {
        id2TimeMap = ChronicleMap.of(Long.class, LongValue.class).putReturnsNull(true).
            entries(System.getProperty("os.name").toLowerCase().contains("win") ? 10_000 : 400_000_000).
            createPersistedTo(time2IdFile);
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
    public ChronicleMap<Long, LongValue> getId2TimeMap() {
      return id2TimeMap;
    }

    @Override
    public ChronicleMap<CharSequence, LongValue> getName2IdMap() {
      return name2IdMap;
    }

    @Override
    public ChronicleMap<Long, IShortProfile> getId2ProfileMap() {
      return id2ProfileMap;
    }
  }
}

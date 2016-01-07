package my.twitter.bolts.profile.chronicle;

import com.google.common.base.Stopwatch;
import my.twitter.beans.IShortProfile;
import my.twitter.topologies.SampleTwitterTopology;
import my.twitter.utils.Constants;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.map.ChronicleMap;
import org.jetbrains.annotations.NotNull;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Created by kkulagin on 12/19/2015.
 */
public abstract class ChronicleDataService {

  public abstract ChronicleMap<CharSequence, LongValue> getName2IdMap();

  public abstract ChronicleMap<LongValue, IShortProfile> getId2ProfileMap();

  public abstract ChronicleMap<LongValue, LongValue> getId2TimeMap();

  public void close() {
    Optional.ofNullable(getId2ProfileMap()).ifPresent(ChronicleMap::close);
    Optional.ofNullable(getId2TimeMap()).ifPresent(ChronicleMap::close);
    Optional.ofNullable(getName2IdMap()).ifPresent(ChronicleMap::close);
  }
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
    private final ChronicleMap<LongValue, LongValue> id2TimeMap;
    private ChronicleMap<LongValue, IShortProfile> id2ProfileMap;

    private DefaultChronicleDataService(Map stormConf) {
      System.out.println("Creating data storage");
      String name2IdFileLocation = (String) stormConf.get(Constants.NAME_2_ID);
      File name2IdFile = getFile(name2IdFileLocation);
      try {
        name2IdMap = ChronicleMap.of(CharSequence.class, LongValue.class).putReturnsNull(true).
            averageKeySize("this_is_18_charctr".length() * 4).
            entries(System.getProperty("os.name").toLowerCase().contains("win") ? 10_000 : 500_000_000).
            createPersistedTo(name2IdFile);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      System.out.println("Created name2id");

      String time2IdFileLocation = (String) stormConf.get(Constants.ID_2_TIME);
      File time2IdFile = getFile(time2IdFileLocation);
      try {
        id2TimeMap = ChronicleMap.of(LongValue.class, LongValue.class).putReturnsNull(true).
//            averageKeySize(Long.BYTES).
            entries(System.getProperty("os.name").toLowerCase().contains("win") ? 10_000 : 500_000_000).
            createPersistedTo(time2IdFile);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      System.out.println("Created id2time");

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
    public ChronicleMap<LongValue, LongValue> getId2TimeMap() {
      return id2TimeMap;
    }

    @Override
    public ChronicleMap<CharSequence, LongValue> getName2IdMap() {
      return name2IdMap;
    }

    @Override
    public ChronicleMap<LongValue, IShortProfile> getId2ProfileMap() {
      return id2ProfileMap;
    }
  }

  public static void main(String[] args) {
    HashMap<Object, Object> config = new HashMap<>();
    final Properties properties = new Properties();
    try (InputStream stream = SampleTwitterTopology.class.getResourceAsStream("/hft.properties")) {
      properties.load(stream);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    config.put(Constants.NAME_2_ID,
        Optional.ofNullable(properties.getProperty(Constants.NAME_2_ID)).
            orElseThrow(() -> new RuntimeException("Property " + Constants.NAME_2_ID + " was not found")));
    config.put(Constants.ID_2_PROFILE,
        Optional.ofNullable(properties.getProperty(Constants.ID_2_PROFILE)).
            orElseThrow(() -> new RuntimeException("Property " + Constants.ID_2_PROFILE + " was not found")));
    config.put(Constants.ID_2_TIME,
        Optional.ofNullable(properties.getProperty(Constants.ID_2_TIME)).
            orElseThrow(() -> new RuntimeException("Property " + Constants.ID_2_TIME + " was not found")));

    System.out.println("Initializing maps");
    Stopwatch started = Stopwatch.createStarted();
    ChronicleDataService instance = null;
    try {
      instance = ChronicleDataService.getInstance(config);
    } finally {
      Optional.ofNullable(instance).ifPresent(ChronicleDataService::close);
    }
    System.out.println("Maps initialized in " + started.elapsed(TimeUnit.SECONDS));
  }
}

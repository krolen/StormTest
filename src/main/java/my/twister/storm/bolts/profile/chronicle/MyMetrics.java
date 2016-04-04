package my.twister.storm.bolts.profile.chronicle;

import my.twister.chronicle.ChronicleDataService;

/**
 * Created by kkulagin on 12/19/2015.
 */
public class MyMetrics implements MyMetricsMBean {
  private ChronicleDataService chronicleDataService;

  public MyMetrics(ChronicleDataService chronicleDataService) {
    this.chronicleDataService = chronicleDataService;
  }

  @Override
  public long name2IdSize() {
    return chronicleDataService.getName2IdMap().longSize();
  }

  @Override
  public long id2TimeSize() {
    return chronicleDataService.getId2TimeMap().longSize();
  }

}

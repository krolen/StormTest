package my.twitter.bolts.profile.chronicle;

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
  public long time2IdSize() {
    return chronicleDataService.getTime2IdMap().longSize();
  }

}

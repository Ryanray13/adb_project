package edu.nyu.cs.adb;

/**
 * Data class abstracts the variable stored in database.
 * 
 * @author Jingxin Zhu
 * @author Wuping  Lei
 *
 */
public class Data {
  private int _value;
  private int _index;
  private boolean _accessible;
  private int _commitTime;

  // record the first time this data becomes unavailable
  private int unavailableTime;

  public Data(int index, int value) {
    _index = index;
    _value = value;
    _accessible = true;
    _commitTime = -1;
    unavailableTime = -1;
  }

  /**
   * Set variable value
   * @param value
   */
  public void setValue(int value) {
    _value = value;
  }

  /**
   * Get the value
   * @return
   */
  public int getValue() {
    return _value;
  }

  /**
   * Set the access status of data.
   * @param access
   */
  public void setAccess(boolean access) {
    _accessible = access;
  }

  /**
   * Get access status of data
   * @return true if data can be accessed. 
   */
  public boolean getAccess() {
    return _accessible;
  }

  /**
   * Set committed time for data.
   * @param time
   */
  public void setCommitTime(int time) {
    _commitTime = time;
  }

  /**
   * Get committed time for data.
   * @return
   */
  public int getCommitTime() {
    return _commitTime;
  }

  /**
   * Get variable index of data.
   * @return
   */
  public int getIndex() {
    return _index;
  }

  /**
   * Get unavailable time.
   * @return
   */
  public int getUnavailableTime() {
    return unavailableTime;
  }

  /**
   * Set unavailable time
   * @param time
   */
  public void setUnavailableTime(int time) {
    //A Data only have one time becoming unavailable
    if (unavailableTime == -1) {
      unavailableTime = time;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || !(o instanceof Data))
      return false;
    Data data = (Data) o;
    return _value == data._value && _index == data._index;
  }

  @Override
  public int hashCode() {
    int result = 17;
    result = 31 * result + Integer.valueOf(_index).hashCode();
    result = 31 * result + Integer.valueOf(_value).hashCode();
    return result;
  }
}

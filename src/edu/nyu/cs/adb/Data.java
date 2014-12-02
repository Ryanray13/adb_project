package edu.nyu.cs.adb;

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

  public void setValue(int value) {
    _value = value;
  }

  public int getValue() {
    return _value;
  }

  public void setAccess(boolean access) {
    _accessible = access;
  }

  public boolean getAccess() {
    return _accessible;
  }

  public void setCommitTime(int time) {
    _commitTime = time;
  }

  public int getCommitTime() {
    return _commitTime;
  }

  public int getIndex() {
    return _index;
  }

  public int getUnavailableTime() {
    return unavailableTime;
  }

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

package edu.nyu.cs.adb;

public class Data {
  private int _value;
  private boolean _accessible;
  private int _commitTime;
  
  public Data(int value){ 
    _value = value;
    _accessible = true;
  }
  
  public void setValue(int value){
    _value = value;
  }
  
  public int getValue(){
    return _value;
  }
  
  public void setAccess(boolean access){
    _accessible = access;
  }
  
  public boolean getAccess(){
    return _accessible;
  }
  
  public void setCommitTime(int time){
    _commitTime = time;
  }
  
  public int getCommitTime(){
    return _commitTime;
  }
  
  @Override
  public boolean equals(Object o){
    if (this == o)
      return true;
    if (o == null || !(o instanceof Data))
      return false;
    Data data = (Data) o;
    return _value == data._value;    
  }
  
  @Override
  public int hashCode(){
    return Integer.valueOf(_value).hashCode();
    
  }
}

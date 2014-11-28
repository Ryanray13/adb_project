package edu.nyu.cs.adb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Transaction {
  
  public static enum Type {
    RO,
    RW,
  };
  
  private int _transactionId;
  private int _timestamp;
  private Type _type; 
  private List<DatabaseManager> _accessSites = new ArrayList<DatabaseManager>();
  private Map<Integer, Integer> _changedVariables = new HashMap<Integer, Integer>();

  public Transaction(int tid, int timestamp, Type type){
    _transactionId = tid;
    _timestamp = timestamp;
    _type = type;
  }
  
  public Type getType(){
    return _type;
  }
  
  public int getTimestamp(){
    return _timestamp;
  }
  
  public int getTranId(){
    return _transactionId;
  }
  
  public List<DatabaseManager> getAccessSites(){
    return _accessSites;
  }
  
  public Map<Integer, Integer> getChangedvariables(){
    return _changedVariables;
  }
  
  @Override
  public boolean equals(Object o){
    if (this == o)
      return true;
    if (o == null || !(o instanceof Transaction))
      return false;
    Transaction tr = (Transaction) o;
    return _transactionId == tr._transactionId ;
  }
  
  @Override
  public int hashCode(){
    return Integer.valueOf(_transactionId).hashCode(); 
  }
}

package edu.nyu.cs.adb;

public class Transaction {
  
  public static enum Type {
    RO,
    RW,
  };
  
  private int _transactionId;
  private int _timestamp;
  private Type _type; 

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
  
  
  @Override
  public boolean equals(Object o){
    if (this == o)
      return true;
    if (o == null || !(o instanceof Transaction))
      return false;
    Transaction tr = (Transaction) o;
    return _transactionId == tr._transactionId 
        && _type == tr._type
        && _timestamp == tr._timestamp;   
  }
  
  @Override
  public int hashCode(){
    int result = 17;
    result = 31 * result + Integer.valueOf(_transactionId).hashCode();
    result = 31 * result + Integer.valueOf(_timestamp).hashCode();
    return result;  
  }
}

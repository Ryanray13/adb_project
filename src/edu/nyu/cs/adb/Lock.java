package edu.nyu.cs.adb;

class Lock {
  static enum Type{
    READ,
    WRITE,
  };
  
  private int _transactionId;
  private Type _type;
  
  public Lock(int tid, Type type){
    _transactionId = tid;
    _type = type;
  }
  
  public int getTranId(){
    return _transactionId;
  }
  
  public Type getType(){
    return _type;
  }
}

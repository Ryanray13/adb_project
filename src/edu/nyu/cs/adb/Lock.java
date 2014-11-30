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
  
  public void setType(Lock.Type type){
    _type = type;
  }
  
  @Override
  public boolean equals(Object o){
    if (this == o)
      return true;
    if (o == null || !(o instanceof Lock))
      return false;
    Lock lc = (Lock) o;
    return _transactionId == lc._transactionId 
        && _type == lc._type;
  }
  
  @Override
  public int hashCode(){
    int result = 17;
    result = 31 * result + Integer.valueOf(_transactionId).hashCode();
    result = 31 * result + _type.hashCode();
    return result; 
  }
}

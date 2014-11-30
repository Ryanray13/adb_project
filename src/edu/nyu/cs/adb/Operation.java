package edu.nyu.cs.adb;

public class Operation {
  
  public static enum Type {
    READ,
    WRITE,
  };

  private int _transactionId;
  private Type _type; 
  private int _varIndex;
  private int _writeValue = 0;
  private int _timestamp;

  public Operation(int tid, int varIndex, int timestamp, Type type) {
    _transactionId = tid;
    _varIndex = varIndex;
    _type = type;
    _timestamp = timestamp;
  }
  
  public Operation(int tid, int varIndex, int timestamp, Type type, int value) {
    _transactionId = tid;
    _varIndex = varIndex;
    _type = type;
    _timestamp = timestamp;
    _writeValue = value;
  }
  
  public Type getType(){
    return _type;
  }
  
  public int getTranId(){
    return _transactionId;
  }
  
  public int getVarIndex(){
    return _varIndex;
  }
  
  public int getWriteValue(){
    return _writeValue;
  }
  
  public int getTimestamp(){
    return _timestamp;
  }
  
  public void setWriteValue(int value){
    if(_type == Type.WRITE){
      _writeValue = value;
    }
  }
  
  @Override
  public String toString() {
    String oper = "[" + _timestamp + "]";
    if (_type == Type.READ) {
      oper += " R";
    } else {
      oper += " W";
    }
    oper += "(T" + _transactionId + ",";
    oper += "x" + _varIndex;
    if (_type == Type.READ) {
      oper += ")";
    } else {
      oper += "," + _writeValue + ")"; 
    }
    return oper;
  }
  
  @Override
  public boolean equals(Object o){
    if (this == o)
      return true;
    if (o == null || !(o instanceof Operation))
      return false;
    Operation op = (Operation) o;
    return _transactionId == op._transactionId 
        && _varIndex == op._varIndex
        && _writeValue == op._writeValue
        && _type == op._type
        && _timestamp == op._timestamp;
  }
  
  @Override
  public int hashCode(){
    int result = 17;
    result = 31 * result + Integer.valueOf(_transactionId).hashCode();
    result = 31 * result + Integer.valueOf(_varIndex).hashCode();
    result = 31 * result + Integer.valueOf(_writeValue).hashCode();
    result = 31 * result + Integer.valueOf(_timestamp).hashCode();
    result = 31 * result + _type.hashCode();
    return result;  
  }
}

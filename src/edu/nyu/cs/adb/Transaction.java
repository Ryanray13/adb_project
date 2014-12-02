package edu.nyu.cs.adb;

public class Transaction {

  public static enum Type {
    RO, RW,
  };

  private int _transactionId;
  private int _timestamp;
  private Type _type;

  public Transaction(int tid, int timestamp, Type type) {
    _transactionId = tid;
    _timestamp = timestamp;
    _type = type;
  }

  public Type getType() {
    return _type;
  }

  public int getTimestamp() {
    return _timestamp;
  }

  public int getTranId() {
    return _transactionId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || !(o instanceof Transaction))
      return false;
    Transaction tr = (Transaction) o;
    return _transactionId == tr._transactionId;
  }

  @Override
  public int hashCode() {
    return Integer.valueOf(_transactionId).hashCode();
  }
}

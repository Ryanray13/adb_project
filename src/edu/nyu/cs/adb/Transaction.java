package edu.nyu.cs.adb;

public class Transaction {
  
  private enum Type {
    READ_ONLY,
    READ_AND_WRITE,
  };
  
  private int _transactionId;
  private int _timestamp;
  private Type _type; 

  public Transaction() {}
}

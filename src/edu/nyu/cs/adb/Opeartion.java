package edu.nyu.cs.adb;


public class Opeartion {
  
  public enum Type {
    READ,
    WRITE,
  };

  private int _transactionId;
  private Type _type; 
  private String _variable;
  private int _writeValue;

  public Opeartion() { }

}

package edu.nyu.cs.adb;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

public class TransactionManager {
  
  private int timestamp;
  
  private Map<Integer, Transaction> transactions = 
      new HashMap<Integer, Transaction>();
  
  private List<DatabaseManager> databaseManagers = 
      new ArrayList<DatabaseManager>();
  
  private List<Transaction> abortedTransactions = 
      new ArrayList<Transaction>();
  
  private List<Transaction> committedTransactions = 
      new ArrayList<Transaction>();

  private List<Operation> waitingOpeartions = new ArrayList<Operation>();
  
  
  /**
   * Parse instructions from input file into database operations.
   * Run those operations. And generate results to standard output.
   * @param filename
   */
  public void run(String inputFile) {
    initialize();
    parseInput(inputFile);
  }
  
  private void initialize() {
    timestamp = 0;
    //TODO: initialize database manager, set site xi to value 10i.
  }

  /**
   * Read instructions from input file. Then parse each instruction into 
   * corresponding database operation. Create Transaction list and Operation
   * list accordingly.
   * @param inputFile
   */
  private void parseInput(String inputFile) {
    try {
      BufferedReader br = new BufferedReader(new FileReader(inputFile));
      while (true) {
        String line = br.readLine();
        if (line == null) break;
        parseLine(line);
        timestamp++;
      }
      br.close();
    } catch (Exception e) {
      System.err.println(e.getMessage());
    }
  }
  
  private List<Operation> parseLine(String line) {
    String[] operationStrs = line.split(";");
    for (String str : operationStrs) {
      int tokenStart = 0;
      int tokenEnd = str.indexOf("(");
      String token = str.substring(tokenStart, tokenEnd);
      int argsEnd = str.indexOf(")");
      String args = str.substring(tokenEnd + 1, argsEnd);
      switch (token) {
        case "begin":
          beginTransaction("RW", args);
          break;
        case "beginRO":
          beginTransaction("RO", args);
          break;
        case "end":
          endTransaction(args);
          break;
        case "W":
          execute(parseWriteOperation(args));
          break;
        case "R":
          execute(parseReadOperation(args));
          break;
        case "fail":
          fail(parseSiteIndex(args));
          break;
        case "recover":
          recover(parseSiteIndex(args));
          break;
        default:
          check(false, "Unrecognised token in " + line);
          break;
      }
    }
    return null;
  }
  
  /** Parse "T*, x*" into corresponding read operation */
  private Operation parseReadOperation(String arg) {
    String[] args = arg.split(",");
    check(args.length == 2, "Unexpected Read " + arg);
    int tid = parseTransactionId(args[0]);
    int var = parseVariable(args[1]);
    return new Operation(tid, var, timestamp, Operation.Type.READ);
  }

  /** Parse "T*, x*, **" into corresponding write operation */
  private Operation parseWriteOperation(String arg) {
    String[] args = arg.split(",");
    check(args.length == 3, "Unexpected Write " + arg);
    int tid = parseTransactionId(args[0]);
    int var = parseVariable(args[1]);
    int writeValue = Integer.parseInt(args[2]);
    return new Operation(tid, var, timestamp, Operation.Type.WRITE, writeValue);
  }

  private void endTransaction(String arg) {
    //TODO: make sure argument is of  ^T[0-9]+$
    int tid = parseTransactionId(arg);
    check(!transactions.containsKey(tid), " already ended.");
    transactions.remove(tid);
  }

  private void beginTransaction(String type, String arg) {
    //TODO: make sure argument is of  ^T[0-9]+$
    int tid = parseTransactionId(arg);
    check(transactions.containsKey(tid), arg + " already begun."); 
    if (type == "RO") {
      transactions.put(tid, 
          new Transaction(tid, timestamp, Transaction.Type.RO));
    } else {
      transactions.put(tid, 
          new Transaction(tid, timestamp, Transaction.Type.RW));
    }
  }
  
  private void execute(Operation operation) {
    
  }

  /**
   * Let the site at given index fail.
   * @param index
   */
  private void fail(int index) {
    databaseManagers.get(index).setSiteStatus(false);
  }

  /**
   * Recovery site at given index.
   * @param index
   */
  private void recover(int index) {
    databaseManagers.get(index).recover();
  }

  /** Convert string to site index. */
  private int parseSiteIndex(String s) {
    return Integer.parseInt(s);
  }
  
  /** Parse transaction id from "T*" */
  private int parseTransactionId(String s) {
    return Integer.parseInt(s.substring(1));
  }
  
  /** Parse variable index from "x*" */
  private int parseVariable(String s) {
    return Integer.parseInt(s.substring(1));
  }
  
  /** If condition is false, print out error message and exit program */
  private void check(boolean condition, String errMsg) {
    if ( ! condition ) {
      System.err.println(errMsg);
      System.exit(-2);
    }
  }

}

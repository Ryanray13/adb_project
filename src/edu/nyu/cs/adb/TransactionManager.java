package edu.nyu.cs.adb;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Queue;
import java.util.Set;

public class TransactionManager {

  // global time stamp
  private int timestamp;

  private BufferedReader br;
  // Map<Transaction id, Transaction>
  private Map<Integer, Transaction> transactions = new HashMap<Integer, Transaction>();

  private List<DatabaseManager> databaseManagers;

  // Map<Variable index, List of sites storing this variable>
  private Map<Integer, List<Integer>> variableMap;

  // List of transaction id that have been aborted.
  private Set<Integer> abortedTransactions = new HashSet<Integer>();

  // List of transaction id that have committed.
  private Set<Integer> committedTransactions = new HashSet<Integer>();

  // TODO: change to queue
  private Queue<Operation> waitingOperations = new LinkedList<Operation>();

  /**
   * Return the current time stamp.
   * 
   * @return time stamp
   */
  public int getCurrentTime() {
    return timestamp;
  }

  /**
   * Check whether there is any running READ_ONLY transaction.
   * 
   * @return
   */
  public boolean hasRunningReadonly() {
    for (Integer tid : transactions.keySet()) {
      if (transactions.get(tid).getType() == Transaction.Type.RO
          && !committedTransactions.contains(tid)
          && !abortedTransactions.contains(tid)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Constructor with initializing.
   */
  public TransactionManager() {
    br = new BufferedReader(new InputStreamReader(System.in));
  }

  public TransactionManager(String inputFile) {
    try {
      br = new BufferedReader(new FileReader(inputFile));
    } catch (Exception e) {
      // TODO: handle exception
    }
  }

  /**
   * Initialize database managers of the given number
   * 
   * @param nDatabaseManager
   *          the number of database managers to be initialized.
   */
  public void init(int nDatabaseManager) {
    timestamp = 0;
    databaseManagers = new ArrayList<DatabaseManager>();
    variableMap = new HashMap<Integer, List<Integer>>();
    for (int index = 1; index <= nDatabaseManager; index++) {
      // TODO: how to pass tm to dm?
      DatabaseManager dm = new DatabaseManager(index, this);
      dm.init();
      databaseManagers.add(dm);
    }
    for (int index = 1; index <= 20; index++) {
      List<Integer> sites = new ArrayList<Integer>();
      if (index % 2 == 1) {
        // store odd variable at (1 + index mod 10) site
        sites.add(1 + index % 10);
      } else {
        // even variable are stored in all sites.
        for (int i = 1; i <= 10; i++) {
          sites.add(i);
        }
      }
      variableMap.put(index, sites);
    }
  }

  // TODO:(1) T1 write x1 twice, lock is on himself.
  // TODO:(2) T is ended while some operations of it are still waiting.
  /**
   * Read content from input file, parse instructions from each line, and run
   * instructions accordingly.
   * 
   * @param inputFile
   */
  public void run() {
    while (true) {
      // batchExecute(waitingOpeartions);
      int waitingSize = waitingOperations.size();
      String line = "";
      try {
        line = br.readLine();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }

      if (line == null || line.equals("exit"))
        break;

      if (line.startsWith("//"))
        continue;
      if (!line.isEmpty()) {
        List<Operation> operations = parseLine(line);
        batchExecute(operations);
      }
      for (int i = 0; i < waitingSize; i++) {
        execute(waitingOperations.poll());
      }
      timestamp++;
    }
    try {
      br.close();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  /**
   * Parse line into list of instructions.Execute instruction. For "begin",
   * "end", "fail", "recover" immediately.
   */
  private List<Operation> parseLine(String line) {
    String[] instructions = line.split(";");
    List<Operation> result = new ArrayList<Operation>();
    for (String instruction : instructions) {
      instruction = instruction.trim();
      int tokenEnd = instruction.indexOf("(");
      String token = instruction.substring(0, tokenEnd);
      int argsEnd = instruction.indexOf(")");
      String arg = instruction.substring(tokenEnd + 1, argsEnd);
      if (token.equals("begin")) {
        beginTransaction("RW", arg);
      } else if (token.equals("beginRO")) {
        beginTransaction("RO", arg);
      } else if (token.equals("end")) {
        endTransaction(arg);
      } else if (token.equals("fail")) {
        fail(parseSiteIndex(arg));
      } else if (token.equals("recover")) {
        recover(parseSiteIndex(arg));
      } else if (token.equals("R")) {
        result.add(parseReadOperation(arg));
      } else if (token.equals("W")) {
        result.add(parseWriteOperation(arg));
      } else if (token.equals("dump")) {
        parseDump(arg);
      } else if (token.equals("querystate")) {
        outputState();
      } else {
        check(false, "Unexpected input: " + instruction);
      }
    }
    return result;
  }

  private void outputState() {
    System.out.println("Transactions committed:");
    for (Integer t : this.committedTransactions) {
      System.out.print("T" + t + " ");
    }
    System.out.println();
    System.out.println("Transactions aborted:");
    for (Integer t : this.abortedTransactions) {
      System.out.print("T" + t + " ");
    }
    System.out.println();
    System.out.println("Transactions still running:");
    for(Integer tid : this.transactions.keySet()){
      if(!committedTransactions.contains(tid) && !abortedTransactions.contains(tid)){
        System.out.print("T" + tid + " ");
      }
    }
    System.out.println();
  }

  /**
   * Execute a batch of operations.
   * 
   * @param operations
   */
  public void batchExecute(List<Operation> operations) {
    for (Operation oper : operations) {
      execute(oper);
    }
  }

  /**
   * Execute a single operation.
   * 
   * @param operation
   */
  public void execute(Operation oper) {
    if (oper.getType() == Operation.Type.READ) {
      read(oper);
    } else {
      write(oper);
    }
  }

  /**
   * 
   * @param oper
   */
  public void write(Operation oper) {
    if (hasAborted(oper.getTranId())) {
      return;
    }
    boolean writable = true;
    boolean allSitesDown = true;
    int varIndex = oper.getVarIndex();
    Set<Integer> conflictTranSet = new HashSet<Integer>();
    List<Integer> sites = getSites(varIndex);
    for (Integer siteIndex : sites) {
      DatabaseManager dm = databaseManagers.get(siteIndex - 1);
      // TODO: assume not all sites are down
      if (dm.getStatus()) {
        allSitesDown = false;
        if (!dm.isWritable(oper.getTranId(), varIndex)) {
          writable = false;
          conflictTranSet.addAll(dm.getConflictTrans(varIndex));
        } 
      }
    }
    if (writable && !allSitesDown) {
      for (Integer siteIndex : sites) {
        DatabaseManager dm = databaseManagers.get(siteIndex - 1);
        if (dm.getStatus()) {
          dm.write(transactions.get(oper.getTranId()), varIndex,
              oper.getWriteValue());
        }
      }
    } else {
      int oldest = getOldestTime(conflictTranSet);
      waitDieProtocol(oper, oldest);
    }
  }

  public int getOldestTime(Set<Integer> conflictTranSet) {
    int result = timestamp + 1;
    Iterator<Integer> it = conflictTranSet.iterator();
    while (it.hasNext()) {
      int time = transactions.get(it.next()).getTimestamp();
      if (time < result) {
        result = time;
      }
    }
    return result;
  }

  /**
   * 
   * @param operation
   */
  public void read(Operation operation) {
    // ignore operation if aborted
    if (hasAborted(operation.getTranId())) {
      return;
    }
    int varIndex = operation.getVarIndex();
    List<Integer> sites = getSites(varIndex);
    for (Integer siteIndex : sites) {
      DatabaseManager dm = databaseManagers.get(siteIndex - 1);
      if (dm.getStatus()) {
        Data data = dm.read(transactions.get(operation.getTranId()), varIndex);
        if (data != null) {
          System.out
              .println("T" + operation.getTranId() + " reads x" + varIndex
                  + " " + data.getValue() + " at site " + dm.getIndex());
          return;
        } else if (dm.getConflictTrans(varIndex) != null
            && dm.getConflictTrans(varIndex).size() != 0) {
          // TODO: wait or abort
          Iterator<Integer> it = dm.getConflictTrans(varIndex).iterator();
          int tid = it.next();
          waitDieProtocol(operation, transactions.get(tid).getTimestamp());
          return;
        }
      }
    }
    // waitingOpeartions.add(operation);
    waitingOperations.offer(operation);
  }

  /**
   * 
   * @param oper
   * @param t
   */
  public boolean waitDieProtocol(Operation oper, int t) {
    if (waitOrDie(oper.getTranId(), t) == "wait") {
      // waitingOpeartions.add(oper);
      // TODO: abort yonger transaction
      waitingOperations.offer(oper);
      return true;
    } else {
      abort(oper.getTranId());
      return false;
    }
  }

  /**
   * If transaction of given id has smaller time stamp than given time stamp,
   * then this transaction should wait. Otherwise, abort this transaction.
   * 
   * @param t
   * @param timestamp
   * @return "wait" or "die"
   */
  public String waitOrDie(int tid, int timestamp) {
    return (transactions.get(tid).getTimestamp() < timestamp) ? "wait" : "die";
  }

  public void parseDump(String arg) {
    String argument = arg.trim();
    if (arg.equals("")) {
      // dump()
      dump();
    } else if (argument.startsWith("x")) {
      dumpVar(parseVariable(argument));
    } else {
      dumpSite(parseSiteIndex(argument));
    }
  }

  public void dump() {
    for (DatabaseManager dm : databaseManagers) {
      System.out.println("Site: " + dm.getIndex());
      dumpSite(dm.getIndex());
    }
  }

  public void dumpSite(int siteIndex) {
    Map<Integer, Data> siteVars = databaseManagers.get(siteIndex - 1)
        .getDataMap();
    List<Integer> indexList = new ArrayList<Integer>(siteVars.keySet());
    Collections.sort(indexList);
    for (Integer varIndex : indexList) {
      System.out.println("x" + varIndex + ": "
          + siteVars.get(varIndex).getValue());
    }
  }

  public void dumpVar(int varIndex) {
    for (DatabaseManager dm : databaseManagers) {
      Data data = dm.dump(varIndex);
      if (data != null) {
        System.out.println("x" + varIndex + ": " + data.getValue()
            + " at site " + dm.getIndex());
      }
    }
  }

  /**
   * Parse transaction id from tidStr and then create new transaction.
   * 
   * @param type
   * @param tidStr
   */
  public void beginTransaction(String type, String tidStr) {
    // TODO: make sure argument is of ^T[0-9]+$
    int tid = parseTransactionId(tidStr);
    // TODO: what if that transaction has finished but still in map ?
    if (transactions.containsKey(tid))
      return;
    if (type == "RO") {
      transactions.put(tid, new Transaction(tid, timestamp,
          Transaction.Type.RO));
    } else {
      transactions.put(tid, new Transaction(tid, timestamp,
          Transaction.Type.RW));
    }
  }

  /**
   * Notify database managers to commit given transaction if that transaction
   * has not been aborted. And put that into committed list.
   */
  public void endTransaction(String tidStr) {
    // TODO: make sure argument is of ^T[0-9]+$
    int tid = parseTransactionId(tidStr);
    if (!hasAborted(tid)) {
      for (DatabaseManager dm : databaseManagers) {
        dm.commit(tid);
      }
      committedTransactions.add(tid);
    }
  }

  /**
   * Notify database managers to abort given transaction and put that
   * transaction put into aborted list.
   * 
   * @param tid
   */
  public void abort(int tid) {
    for (DatabaseManager dm : databaseManagers) {
      dm.abort(tid);
    }
    abortedTransactions.add(tid);
  }

  /**
   * Let the site at given index fail. Abort all transactions that have accessed
   * that site immediately.
   * 
   * @param index
   */
  public void fail(int siteIndex) {
    List<Integer> accessedTransactions = databaseManagers.get(siteIndex - 1)
        .getAccessedTransaction();
    for (Integer tid : accessedTransactions) {
      abortedTransactions.add(tid);
      this.abort(tid);
    }
    databaseManagers.get(siteIndex - 1).fail();
  }

  /**
   * Recovery site at given index.
   * 
   * @param index
   */
  public void recover(int index) {
    databaseManagers.get(index - 1).recover();
  }

  /** Return all sites that storing given variable. */
  public List<Integer> getSites(int varIndex) {
    return variableMap.get(varIndex);
  }

  public boolean hasAborted(int tid) {
    return abortedTransactions.contains(tid);
  }

  /** Parse transaction id from "T*" string */
  public int parseTransactionId(String s) {
    return Integer.parseInt(s.trim().substring(1));
  }

  /** Parse site index out of string. */
  public int parseSiteIndex(String s) {
    return Integer.parseInt(s.trim());
  }

  /** Parse variable index from "x*" */
  public int parseVariable(String s) {
    return Integer.parseInt(s.trim().substring(1));
  }

  /** Parse "T*, x*" into corresponding read operation */
  public Operation parseReadOperation(String arg) {
    String[] args = arg.split(",");
    check(args.length == 2, "Unexpected Read " + arg);
    int tid = parseTransactionId(args[0]);
    int var = parseVariable(args[1]);
    return new Operation(tid, var, timestamp, Operation.Type.READ);
  }

  /** Parse "T*, x*, **" into corresponding write operation */
  public Operation parseWriteOperation(String arg) {
    String[] args = arg.split(",");
    check(args.length == 3, "Unexpected Write " + arg);
    int tid = parseTransactionId(args[0]);
    int var = parseVariable(args[1]);
    int writeValue = Integer.parseInt(args[2].trim());
    return new Operation(tid, var, timestamp, Operation.Type.WRITE, writeValue);
  }

  /** If condition is false, print out error message and exit program */
  public void check(boolean condition, String errMsg) {
    /*
     * if ( ! condition ) { System.err.println(errMsg); System.exit(-2); }
     */
  }

}

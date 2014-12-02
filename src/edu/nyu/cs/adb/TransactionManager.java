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

/**
 * Transaction manager (TM) acts as the interface between database user and
 * database. TM takes the input from user and execute it accordingly together
 * with database managers (DM).
 * 
 * @author Jingxin Zhu
 * @author Wuping Lei
 */
public class TransactionManager {

  // Global time stamp
  private int timestamp;

  // General input reader
  private BufferedReader br;

  // trace whether there is a transaction abort or commit or site recover
  // Which might cause waiting transaction be able to execute
  private boolean commitOrAbortOrRecover;

  // Map<Transaction id, Transaction>.
  private Map<Integer, Transaction> transactions = new HashMap<Integer, Transaction>();

  private List<DatabaseManager> databaseManagers;

  // Map<Variable index, List of sites storing this variable>.
  private Map<Integer, List<Integer>> variableMap;

  // List of transaction id that have been aborted.
  private Set<Integer> abortedTransactions = new HashSet<Integer>();

  // List of transaction id that have committed.
  private Set<Integer> committedTransactions = new HashSet<Integer>();

  // Queue of all waiting operations.
  private Queue<Operation> waitingOperations = new LinkedList<Operation>();

  /**
   * Constructor with standard input .
   */
  public TransactionManager() {
    br = new BufferedReader(new InputStreamReader(System.in));
  }

  /**
   * Constructor with file input .
   */
  public TransactionManager(String inputFile) {
    try {
      br = new BufferedReader(new FileReader(inputFile));
    } catch (Exception e) {
      System.err.println(e.getMessage());
    }
  }

  /**
   * Return the current time stamp.
   * 
   * @return current time stamp.
   */
  public int getCurrentTime() {
    return timestamp;
  }

  /**
   * Check whether there is any running READ_ONLY transaction.
   * 
   * @return true if there is running READ_ONLY transaction.
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
   * Initialize database managers of the given number.
   * 
   * @param nDatabaseManager
   *          the number of database managers to be initialized.
   */
  public void init(int nDatabaseManager) {
    timestamp = 0;
    databaseManagers = new ArrayList<DatabaseManager>();
    variableMap = new HashMap<Integer, List<Integer>>();
    for (int index = 1; index <= nDatabaseManager; index++) {
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

  /**
   * Read contents from standard input or input file. Parse instructions, and
   * then execute operations accordingly.
   */
  public void run() {
    try {
      while (true) {
        //Reset this boolean
        commitOrAbortOrRecover = false;
        String line = br.readLine();
        if (line == null || line.contains("exit"))
          break;
        if (line.startsWith("//"))
          continue;
        if (!line.isEmpty()) {
          List<Operation> operations = parseLine(line);
          batchExecute(operations);
        }

        // If there is a commit or abort or restart, re-issue all the waiting
        // operations
        while (commitOrAbortOrRecover == true) {
          commitOrAbortOrRecover = false;
          int size = waitingOperations.size();
          for (int i = 0; i < size; i++) {
            execute(waitingOperations.poll());
          }
        }
        timestamp++;
      }
      br.close();
    } catch (IOException e) {
      System.err.println(e.getMessage());
    }
  }

  /**
   * Parse line into list of operations. Execute instructions for "begin",
   * "end", "fail", "recover" immediately.
   */
  private List<Operation> parseLine(String line) {
    line = line.toLowerCase();
    String[] instructions = line.replaceAll("\\s+", "").split(";");
    List<Operation> result = new ArrayList<Operation>();
    for (String instruction : instructions) {
      String token = "";
      String arg = "";
      int tokenEnd = instruction.indexOf("(");
      int argsEnd = instruction.indexOf(")");
      if (tokenEnd == -1 || argsEnd == -1) {
        System.out.println("Unexpected: " + instruction);
        break;
      }
      token = instruction.substring(0, tokenEnd);
      arg = instruction.substring(tokenEnd + 1, argsEnd);
      if (token.equals("begin")) {
        beginTransaction("RW", arg);
      } else if (token.equals("beginro")) {
        beginTransaction("RO", arg);
      } else if (token.equals("end")) {
        endTransaction(arg);
      } else if (token.equals("fail")) {
        fail(parseSiteIndex(arg));
      } else if (token.equals("recover")) {
        recover(parseSiteIndex(arg));
      } else if (token.equals("r")) {
        result.add(parseReadOperation(arg));
      } else if (token.equals("w")) {
        result.add(parseWriteOperation(arg));
      } else if (token.equals("dump")) {
        parseDump(arg);
      } else if (token.equals("querystate")) {
        queryState();
      } else if (token.equals("restart")) {
        restart();
      } else {
        System.out.println("Unexpected input: " + instruction);
      }
    }
    return result;
  }

  /**
   * Parse transaction id from tidStr and then create new transaction.
   * 
   * @param type
   *          READ_ONLY or READ_WRITE
   * 
   * @param tidStr
   *          string that containing transaction id.
   */
  public void beginTransaction(String type, String tidStr) {
    int tid = parseTransactionId(tidStr);
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
   * has not been aborted and put that into committed list. If RO commits and no
   * more RO left, then clear all the copies in DMs.
   * 
   * @param tidStr
   */
  public void endTransaction(String tidStr) {
    int tid = parseTransactionId(tidStr);
    if (!hasAborted(tid)) {
      for (DatabaseManager dm : databaseManagers) {
        if (dm.getStatus()) {
          dm.commit(tid);
        }
      }
      System.out.println("T" + tid + " is committed");
      //If a Read-only transaction commits, check whether this is the last read-only
      //If so, let all the DM clear all the old versions
      commitOrAbortOrRecover = true;
      committedTransactions.add(tid);
      if (transactions.containsKey(tid)) {
        if (transactions.get(tid).getType() == Transaction.Type.RO
            && !hasRunningReadonly()) {
          for (DatabaseManager dm : databaseManagers) {
            dm.clearAllVersions();
          }
        }
      }
    }
  }

  /**
   * Let the site at given index fail. Abort all transactions that have accessed
   * that site immediately.
   * 
   * @param siteIndex
   */
  public void fail(int siteIndex) {
    List<Integer> accessedTransactions = databaseManagers.get(siteIndex - 1)
        .getAccessedTransaction();
    //If site fails, abort all the transaction accessed to the site
    for (Integer tid : accessedTransactions) {
      abortedTransactions.add(tid);
      abort(tid);
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
    commitOrAbortOrRecover = true;
  }

  /** Restart database, clear current states. */
  public void restart() {
    init(10);
    timestamp = -1;
    transactions.clear();
    committedTransactions.clear();
    abortedTransactions.clear();
    waitingOperations.clear();
  }

  /**
   * Print out current query state: committed transactions, aborted
   * transactions, and running transactions and all the site status.
   */
  public void queryState() {
    System.out.print("Transactions committed: ");
    for (Integer t : committedTransactions) {
      System.out.print("T" + t + " ");
    }
    System.out.println();
    System.out.print("Transactions aborted: ");
    for (Integer t : abortedTransactions) {
      System.out.print("T" + t + " ");
    }
    System.out.println();
    System.out.print("Transactions still running: ");
    for (Integer tid : transactions.keySet()) {
      if (!committedTransactions.contains(tid)
          && !abortedTransactions.contains(tid)) {
        System.out.print("T" + tid + " ");
      }
    }
    System.out.println();
    System.out.println("Site status:");
    for (DatabaseManager dm : databaseManagers) {
      System.out.print("Site" + dm.getIndex());
      if (dm.getStatus()) {
        System.out.print("(Up); ");
      } else {
        System.out.print("(down); ");
      }
    }
  }

  /*
   * Execute a batch of operations.
   * 
   * @param operations
   */
  private void batchExecute(List<Operation> operations) {
    for (Operation oper : operations) {
      execute(oper);
    }
  }

  /*
   * Execute a single operation.
   * 
   * @param operation
   */
  private void execute(Operation oper) {
    if (oper.getType() == Operation.Type.READ) {
      read(oper);
    } else {
      write(oper);
    }
  }

  /**
   * Try to execute write operation. If succeed, write the value to sites.
   * Otherwise, decide the transaction to wait or die.
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
      if (dm.getStatus()) {
        allSitesDown = false;
        if (!dm.isWritable(oper.getTranId(), varIndex)) {
          writable = false;
          //If have conflict, add all conflict transaction ids into conflictTranSet
          conflictTranSet.addAll(dm.getConflictTrans(oper.getTranId(),
              varIndex));
        }
      }
    }
    //If no conflict and not all sites are down
    if (writable && !allSitesDown) {
      for (Integer siteIndex : sites) {
        DatabaseManager dm = databaseManagers.get(siteIndex - 1);
        if (dm.getStatus()) {
          dm.write(transactions.get(oper.getTranId()), varIndex,
              oper.getWriteValue());
        }
      }
    } else {
      //Get the oldest transaction from conflictTranSet, if get a null object,
      //indicates no conflict and all sites are down which makes the transaction wait
      //This is the only place where waitDieProtocol will have a null transaction as parameter
      Transaction oldest = getOldestTransaction(conflictTranSet);
      waitDieProtocol(oper, oldest);
    }
  }

  // Get the oldest transaction from all given conflicting transactions.
  // If conflictTranset is empty, return null
  private Transaction getOldestTransaction(Set<Integer> conflictTranSet) {
    int result = timestamp + 1;
    Transaction oldest = null;
    for (Integer tid : conflictTranSet) {
      int time = transactions.get(tid).getTimestamp();
      if (time < result) {
        result = time;
        oldest = transactions.get(tid);
      }
    }
    return oldest;
  }

  /**
   * Try to execute read operation. If succeed, read the value. Otherwise,
   * decide transaction to wait or die.
   * 
   * @param operation
   */
  public void read(Operation operation) {
    // ignore operation if aborted
    int tranid = operation.getTranId();
    if (hasAborted(tranid)) {
      return;
    }
    int varIndex = operation.getVarIndex();
    List<Integer> sites = getSites(varIndex);
    for (Integer siteIndex : sites) {
      DatabaseManager dm = databaseManagers.get(siteIndex - 1);
      if (dm.getStatus()) {
        Data data = dm.read(transactions.get(tranid), varIndex);
        if (data != null) {
          System.out.println("T" + tranid + " reads x" + varIndex + ": "
              + data.getValue() + " at site " + dm.getIndex());
          return;
        } else if (dm.getConflictTrans(tranid, varIndex) != null
            && dm.getConflictTrans(tranid, varIndex).size() != 0) {
          
          //If the Data returned is null, either because conflict or Data is unavailable
          //If Data is unavailable, there will be no conflict
          //If conflicts, can only conflict with one transaction who has the write Lock
          Iterator<Integer> it = dm.getConflictTrans(tranid, varIndex)
              .iterator();
          int tid = it.next();
          waitDieProtocol(operation, transactions.get(tid));
          return;
        }
      }
    }
    System.out
        .println("T"
            + operation.getTranId()
            + " should wait because all the sites are down or all the variable are unavailable." 
            + "(" + operation.toString() + ")");
    waitingOperations.offer(operation);
  }

  /*
   * If the transaction associated with given operation is older than given t,
   * then given operation should "wait". Otherwise, abort transaction.
   * 
   * @param oper
   * 
   * @param t
   */
  private boolean waitDieProtocol(Operation oper, Transaction t) {
    //If transaction is null, only happens when all sites are down
    if (t == null) {
      System.out.println("T" + oper.getTranId()
          + " should wait because all the sites are down. " + "(" + oper.toString() + ")");
      waitingOperations.offer(oper);
      return true;
    }
    if (transactions.get(oper.getTranId()).getTimestamp() < t.getTimestamp()) {
      // should wait
      System.out.println("T" + oper.getTranId() + " should wait for T"
          + t.getTranId() + " (" + oper.toString() + ")");
      waitingOperations.offer(oper);
      return true;
    } else {
      System.out.println("T" + oper.getTranId() + " should abort because T"
          + t.getTranId() + " is older (" + oper.toString() + ")");
      abort(oper.getTranId());
      return false;
    }
  }

  // Parse dump-related commands.
  private void parseDump(String arg) {
    if (arg.equals("")) {
      dump();
    } else if (arg.startsWith("x")) {
      dumpVar(parseVariable(arg));
    } else {
      dumpSite(parseSiteIndex(arg));
    }
  }

  // Print all committed values af each variable at each site.
  public void dump() {
    for (DatabaseManager dm : databaseManagers) {
      dumpSite(dm.getIndex());
    }
  }

  // Print all committed values of all variables at given site.
  public void dumpSite(int siteIndex) {
    Map<Integer, Data> siteVars = databaseManagers.get(siteIndex - 1)
        .getDataMap();
    List<Integer> indexList = new ArrayList<Integer>(siteVars.keySet());
    Collections.sort(indexList);
    if (!databaseManagers.get(siteIndex - 1).getStatus()) {
      System.out.println("====== Site: " + siteIndex + "(Down) ======");
    } else {
      System.out.println("====== Site: " + siteIndex + " ======");
    }

    for (Integer varIndex : indexList) {
      System.out.print("x" + varIndex + ": "
          + siteVars.get(varIndex).getValue());
      if (!siteVars.get(varIndex).getAccess()) {
        System.out.print(" (unavailable)");
      }
      System.out.println();
    }
  }

  // Print all committed values of given variable.
  public void dumpVar(int varIndex) {
    for (DatabaseManager dm : databaseManagers) {
      Data data = dm.dump(varIndex);
      if (data != null) {
        System.out.print("x" + varIndex + ": " + data.getValue() + " at site "
            + dm.getIndex());
        if (!data.getAccess()) {
          System.out.print(" (unavailable)");
        }
        System.out.println();
      }
    }
  }

  /**
   * Notify database managers to abort given transaction and put that
   * transaction put into aborted list.
   * 
   * @param tid
   *          transaction id
   */
  public void abort(int tid) {
    for (DatabaseManager dm : databaseManagers) {
      if (dm.getStatus()) {
        dm.abort(tid);
      }
    }
    abortedTransactions.add(tid);
    commitOrAbortOrRecover = true;
  }

  // Return all sites that storing given variable.
  private List<Integer> getSites(int varIndex) {
    return variableMap.get(varIndex);
  }

  private boolean hasAborted(int tid) {
    return abortedTransactions.contains(tid);
  }

  // Parse transaction id from "T*" string
  private int parseTransactionId(String s) {
    return Integer.parseInt(s.substring(1));
  }

  // Parse site index out of string.
  private int parseSiteIndex(String s) {
    return Integer.parseInt(s);
  }

  // Parse variable index from "x*"
  private int parseVariable(String s) {
    return Integer.parseInt(s.substring(1));
  }

  // Parse "T*, x*" into corresponding read operation
  private Operation parseReadOperation(String arg) {
    String[] args = arg.split(",");
    check(args.length == 2, "Unexpected Read " + arg);
    int tid = parseTransactionId(args[0]);
    int var = parseVariable(args[1]);
    return new Operation(tid, var, timestamp, Operation.Type.READ);
  }

  // Parse "T*, x*, **" into corresponding write operation
  private Operation parseWriteOperation(String arg) {
    String[] args = arg.split(",");
    check(args.length == 3, "Unexpected Write " + arg);
    int tid = parseTransactionId(args[0]);
    int var = parseVariable(args[1]);
    int writeValue = Integer.parseInt(args[2]);
    return new Operation(tid, var, timestamp, Operation.Type.WRITE, writeValue);
  }

  // If condition is false, print out error message and exit program
  private void check(boolean condition, String errMsg) {
    if (!condition) {
      System.err.println(errMsg);
      System.exit(-2);
    }
  }
}

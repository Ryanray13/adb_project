package edu.nyu.cs.adb;

/**
 * This class is the main entry class for distributed database system.
 * -----------------------------------------------------------------------
 * Description: 
 * Including one central transaction manager (TM) and several database
 * managers (DM), the database system takes advantage of available copies 
 * algorithm to increase availability, wait-die protocol to avoid deadlock,
 * multiversion concurrency control to speed READONLY transactions, and
 * failure-recovery to resist unexpected failures. 
 * -----------------------------------------------------------------------
 * Usage:
 * Assuming user has changed directory to parent directory of src:
 * To compile:
 * 
 *    javac src/edu/nyu/cs/adb/*.java
 *    
 * To use:
 * Option 1) read instructions from standard input.
 * 
 *    java -cp src edu.nyu.cs.adb.Database
 *    
 * Option 2) read instrutions from file.
 * 
 *    java -cp src edu.nyu.cs.adb.Database <PATH_TO_INPUTFILE>
 *  
 * -----------------------------------------------------------------------
 * @author Jingxin Zhu (jz1371)
 * @author Wuping Lei  (wl1002)
 * 
 * Date: 2014/12/02
 * @version 1.0
 */
public class Database {

  public static void main(String[] args) {
    
    TransactionManager tm;
    
    if (args.length == 0) {
      tm = new TransactionManager();
    } else{
      tm = new TransactionManager(args[0]);
    }
    
    int nDatabaseManagers = 10;    
    tm.init(nDatabaseManagers);  
    System.out.println("Database starts, use exit() to exit database >>");
    System.out.println();
    tm.run();
    
  }
}

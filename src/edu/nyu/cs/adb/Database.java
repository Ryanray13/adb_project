package edu.nyu.cs.adb;

/**
 * Encapsulate abstract objects into database.
 * Start database.
 */
public class Database {

  public static void main(String[] args) {

      TransactionManager tm ;
    if (args.length == 0) {
      tm = new TransactionManager();
    } else {
      tm = new TransactionManager(args[0]);
    }

    tm.init(10);

    tm.run();
    
  }
  
  private static String parseCommand(String[] args) {
    if (args.length == 0) {
      System.err.println("Please provide one input file");
      System.exit(-1);
    } 
    return args[0];
  }

}

package edu.nyu.cs.adb;

/**
 * Encapsulate abstract objects into database.
 * Start database.
 */
public class Database {

  public static void main(String[] args) {

    //TODO: read in standard input 
    String filename = parseCommand(args);

    new TransactionManager().run(filename);

  }
  
  private static String parseCommand(String[] args) {
    if (args.length == 0) {
      System.err.println("Please provide one input file");
      System.exit(-1);
    } else if (args.length > 1) {
      System.err.println("Please provide only one input file");
      System.exit(-1);
    } 
    return args[0];
  }

}

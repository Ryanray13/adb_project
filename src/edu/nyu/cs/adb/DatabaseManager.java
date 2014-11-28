package edu.nyu.cs.adb;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DatabaseManager {
  
  private boolean _siteStatus;
  private int _lastRecoveryTime;
  private Map<Integer, Data> _dataMap = new HashMap<Integer, Data>();
  private Map<Integer, List<Lock>> _lockTable = new HashMap<Integer, List<Lock>>();
  
  private static Map<Integer, List<Data>> _availableCopies = new HashMap<Integer, List<Data>>();
}

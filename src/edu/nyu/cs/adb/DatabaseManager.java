package edu.nyu.cs.adb;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DatabaseManager {

  private boolean _siteStatus;
  private int _siteIndex;
  private int _lastRecoveryTime;
  private Map<Integer, Data> _dataMap = new HashMap<Integer, Data>();
  private Map<Integer, List<Lock>> _lockTable = new HashMap<Integer, List<Lock>>();

  private static Map<Integer, List<Data>> _availableCopies = new HashMap<Integer, List<Data>>();

  public DatabaseManager(int index) {
    _siteStatus = true;
    _lastRecoveryTime = -1;
    _siteIndex = index;
  }

  public void init() {
    for (int i = 1; i <= 20; i++) {
      if (i % 2 == 0) {
        _dataMap.put(i, new Data(i, 10 * i));
      } else if ((1 + i % 10) == _siteIndex) {
        _dataMap.put(i, new Data(i, 10 * i));
      }
    }
  }

  public boolean getStatus() {
    return _siteStatus;
  }

  public void setStatus(boolean status) {
    _siteStatus = status;
  }

  public Map<Integer, Data> getDataMap() {
    return _dataMap;
  }

  public int getRecoverTime() {
    return _lastRecoveryTime;
  }

  public void setRecoverTime(int time) {
    _lastRecoveryTime = time;
  }

  /* set corresponding lock for given variable */
  private void setLock(int tid, int variableIndex, Lock.Type type) {

  }

  /*
   * release all the lock hel by given transaction when transaction abort or
   * commit
   */
  private void releaseAllLocks(int tid) {

  }

  /** recover this site, clear the lock table */
  public void recover() {

  }

  /**
   * Given a variable index, get the committed values of the variable Called by
   * TM
   * 
   * @param varIndex
   * @return data
   */
  public Data dump(int varIndex) {
    return null;
  }

  /**
   * Commit the given transaction, write all the values in site
   * 
   * @param t
   * @return boolean
   */
  public boolean commit(Transaction t) {
    return true;
  }

  /**
   * Given a variable index, read the data, if fails return null
   * 
   * @param t
   * @param varIndex
   * @return data
   */
  public Data read(Transaction t, int varIndex) {
    return null;
  }

  /**
   * Given a variable index, value to write and transaction, update the variable
   * set the lock But since we use redo, no undo thus we update it in
   * transaction first, when commit writes all the values into database
   * 
   * @param t
   * @param varIndex
   * @param value
   */
  public void write(Transaction t, int varIndex, int value) {
  }

  /**
   * Given a variable index and transaction, check whether the transaction can
   * write the variable, i.e. whether it can get the lock
   * 
   * @param t
   * @param varIndex
   * @return true if can write, false if can't
   */
  public boolean whetherCanWrite(Transaction t, int varIndex) {
    return false;
  }

  /**
   * Given a variable index return the list of transaction ids that have
   * conflicts, i.e. have the lock on this variable.
   * 
   * @param varIndex
   * @return list of transaction ids
   */
  List<Integer> getConflictTrans(int varIndex) {
    return null;
  }

}

package edu.nyu.cs.adb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DatabaseManager {

  private boolean _siteStatus;
  private int _siteIndex;
  private TransactionManager _tm;
  private Map<Integer, List<Data>> _dataMap = new HashMap<Integer, List<Data>>();
  private Map<Integer, List<Lock>> _lockTable = new HashMap<Integer, List<Lock>>();
  private Set<Integer> _accessedTransactions = new HashSet<Integer>();

  public DatabaseManager(int index, TransactionManager tm) {
    _siteStatus = true;
    _siteIndex = index;
    _tm = tm;
  }

  public void init() {
    for (int i = 1; i <= 20; i++) {
      List<Data> dataList = new ArrayList<Data>();
      if (i % 2 == 0 || (1 + i % 10) == _siteIndex) {
        dataList.add(new Data(i, 10 * i));
        _dataMap.put(i, dataList);
      }
    }
  }

  public boolean getStatus() {
    return _siteStatus;
  }

  public void setStatus(boolean status) {
    _siteStatus = status;
  }

  /* set corresponding lock for given variable */
  private void setLock(int tid, int varIndex, Lock.Type type) {
    List<Lock> list = null;
    if(_lockTable.containsKey(varIndex)){
      list = _lockTable.get(varIndex);
      list.add(new Lock(tid, type));
    }else{
      list = new ArrayList<Lock>();
      list.add(new Lock(tid, type));
      _lockTable.put(varIndex, list);
    }
  }

  /*
   * release all the lock hel by given transaction when transaction abort or
   * commit
   */
  private void releaseAllLocks(int tid) {

  }

  /** recover this site*/
  public void recover() {

  }

  /** fail this site, clear the lock table */
  public void fail() {

  }
  
  /**
   * Return the data map this site has
   * @return dataMap
   */
  public Map<Integer, Data> getDataMap() {
    Map<Integer, Data> result = new HashMap<Integer, Data>();
    for(Integer key: _dataMap.keySet()){
      List<Data> list = _dataMap.get(key);
      result.put(key, list.get(list.size()-1));
    }
    return result;
  }
  
  /**
   * Return all the transactions that have accessed this site
   * When the site fails, those transactions need to abort
   * @return accessed transactions list
   */
  public List<Integer> getAccessedTransaction() {
    return new ArrayList<Integer>(_accessedTransactions);
  }
  
  /**
   * Given a variable index, get the committed values of the variable.
   *  Called by TM
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
   */
  public void commit(Transaction t) {
    return;
  } 
  
  /**
   * Abort the given transaction
   * 
   * @param t
   */
  public void abort(Transaction t) {
    return;
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
  public boolean isWritable(Transaction t, int varIndex) {
    return false;
  }

  /**
   * Given a variable index return the list of transaction ids that have
   * conflicts, i.e. have the lock on this variable.
   * 
   * @param varIndex
   * @return list of transaction ids
   */
  Set<Integer> getConflictTrans(int varIndex) {
    return null;
  }

}

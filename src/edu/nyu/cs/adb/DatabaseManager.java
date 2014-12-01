package edu.nyu.cs.adb;

import java.util.ArrayList;
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
  private Map<Integer, Data> _uncommitDataMap = new HashMap<Integer, Data>();
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
  
  public int getIndex() {
    return _siteIndex;
  }

  public boolean getStatus() {
    return _siteStatus;
  }

  public void setStatus(boolean status) {
    _siteStatus = status;
  }

  /* set corresponding lock for given variable */
  private void setLock(int tid, int varIndex, Lock.Type type) {
    List<Lock> lockList = null;
    if (_lockTable.containsKey(varIndex)) {
      lockList = _lockTable.get(varIndex);
      lockList.add(new Lock(tid, type));
    } else {
      lockList = new ArrayList<Lock>();
      lockList.add(new Lock(tid, type));
      _lockTable.put(varIndex, lockList);
    }
  }

  /* Get the corresponding lock for given variable and given transaction */
  private Lock getLock(int tid, int varIndex) {
    if (_lockTable.containsKey(varIndex)) {
      List<Lock> lockList = _lockTable.get(varIndex);
      for (Lock lc : lockList) {
        if (lc.getTranId() == tid) {
          return lc;
        }
      }
    }
    return null;
  }

  /* check whether there is conflict with that tid */
  private boolean hasConflict(int tid, int varIndex, Lock.Type type) {
    if (!_lockTable.containsKey(varIndex)) {
      return false;
    }
    List<Lock> lockList = _lockTable.get(varIndex);
    if (type == Lock.Type.READ) {
      for (Lock lc : lockList) {
        if (lc.getTranId() != tid && lc.getType() == Lock.Type.WRITE) {
          return true;
        }
      }
      return false;
    } else {
      for (Lock lc : lockList) {
        if (lc.getTranId() != tid) {
          return true;
        }
      }
      return false;
    }
  }

  /*
   * release all the lock held by given transaction when transaction abort or
   * commit
   */
  private void releaseAllLocks(int tid) {
    List<Lock> lockList = null;
    for (Integer varIndex : _lockTable.keySet()) {
      lockList = _lockTable.get(varIndex);
      int size = lockList.size();
      for (int i = size - 1; i >= 0; i--) {
        if (lockList.get(i).getTranId() == tid) {
          lockList.remove(i);
          break;
        }
      }
    }
  }

  /** recover this site */
  public void recover() {
    _siteStatus = true;
    for (Integer varIndex : _dataMap.keySet()) {
      List<Data> dataList;
      if (varIndex % 2 == 0) {
        dataList = _dataMap.get(varIndex);
        dataList.get(dataList.size() - 1).setAccess(false);
      }
    }
  }

  /** fail this site, clear the lock table */
  public void fail() {
    _siteStatus = false;
    _lockTable.clear();
    _accessedTransactions.clear();
    _uncommitDataMap.clear();
  }

  /**
   * Return the data map this site has
   * 
   * @return dataMap
   */
  public Map<Integer, Data> getDataMap() {
    Map<Integer, Data> result = new HashMap<Integer, Data>();
    for (Integer varIndex : _dataMap.keySet()) {
      List<Data> dataList = _dataMap.get(varIndex);
      result.put(varIndex, dataList.get(dataList.size() - 1));
    }
    return result;
  }

  /**
   * Return all the transactions that have accessed this site When the site
   * fails, those transactions need to abort
   * 
   * @return accessed transactions list
   */
  public List<Integer> getAccessedTransaction() {
    return new ArrayList<Integer>(_accessedTransactions);
  }

  /**
   * Given a variable index, get the committed values of the variable.
   * 
   * @param varIndex
   * @return data
   */
  public Data dump(int varIndex) {
    if (_dataMap.containsKey(varIndex)) {
      return getLastCommitData(varIndex);
    } else {
      return null;
    }
  }

  /* get the last commit data of that index */
  private Data getLastCommitData(int varIndex) {
    List<Data> dataList = _dataMap.get(varIndex);
    return dataList.get(dataList.size() - 1);
  }

  /**
   * clear all the old copies of the variable on this site
   */
  public void clearAllCopies() {
    for (Integer varIndex : _dataMap.keySet()) {
      List<Data> dataList = _dataMap.get(varIndex);
      while (dataList.size() > 1) {
        dataList.remove(0);
      }
    }
  }

  /**
   * Commit the given transaction, write all the values in site And release all
   * the lock it holds
   * 
   * @param tid transaction id
   */
  public void commit(int tid) {
    List<Lock> lockList = null;
    boolean hasRO = _tm.hasRunningReadonly();
    for (Integer varIndex : _lockTable.keySet()) {
      lockList = _lockTable.get(varIndex);
      int size = lockList.size();
      for (int i = size - 1; i >= 0; i--) {
        Lock lc = lockList.get(i);
        if (lc.getTranId() == tid) {
          if (lc.getType() == Lock.Type.WRITE) {
            if (_uncommitDataMap.containsKey(varIndex)) {
              List<Data> dataList = _dataMap.get(varIndex);
              Data d = _uncommitDataMap.get(varIndex);
              d.setCommitTime(_tm.getCurrentTime());
              if (!hasRO) {
                dataList.clear();
              }
              dataList.add(d);
              _uncommitDataMap.remove(varIndex);
            }
          }
          lockList.remove(i);
          break;
        }
      }
    }
  }

  /**
   * Abort the given transaction, release all the locks it holds
   * 
   * @param tid transaction id
   */
  public void abort(int tid) {
    List<Lock> lockList = null;
    for (Integer varIndex : _lockTable.keySet()) {
      lockList = _lockTable.get(varIndex);
      int size = lockList.size();
      for (int i = size - 1; i >= 0; i--) {
        if (lockList.get(i).getTranId() == tid) {
          lockList.remove(i);
          _uncommitDataMap.remove(varIndex);
          break;
        }
      }
    }
  }

  /**
   * Given a variable index, read the data, if fails return null
   * 
   * @param t
   * @param varIndex
   * @return data
   */
  public Data read(Transaction t, int varIndex) {
    if (!_dataMap.containsKey(varIndex)) {
      return null;
    }
    int tid = t.getTranId();
    
    if (t.getType() == Transaction.Type.RW) {
      boolean hasConflict = false;
      Lock lock = null;
      if (_lockTable.containsKey(varIndex)) {
        List<Lock> lockList = _lockTable.get(varIndex);
        for (Lock lc : lockList) {
          if (lc.getTranId() == tid) {
            lock = lc;
          } else {
            if (lc.getType() == Lock.Type.WRITE) {
              hasConflict = true;
            }
          }
        }
      }

      if (hasConflict) {
        return null;
      } else {
        if (lock == null || lock.getType() == Lock.Type.READ) {
          Data d = getLastCommitData(varIndex);
          if (d.getAccess()) {
            if (lock == null) {
              setLock(tid, varIndex, Lock.Type.READ);
            }
            _accessedTransactions.add(tid);
          }
          return d;
        } else {
          _accessedTransactions.add(tid);
          return _uncommitDataMap.get(varIndex);
        }
      }
    } else {
      List<Data> dataList = _dataMap.get(varIndex);
      Data d = null;
      int ttime = t.getTimestamp();
      for(Data dt : dataList){
        if(dt.getCommitTime() <= ttime){
          d = dt;
        }else{
          break;
        }
      }
      return d;
    }
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
    int tid = t.getTranId();
    Lock lc = getLock(tid, varIndex);
    if (hasConflict(tid, varIndex, Lock.Type.WRITE)) {
      return;
    }
    if (lc == null) {
      setLock(tid, varIndex, Lock.Type.WRITE);
    } else {
      if (lc.getType() == Lock.Type.READ) {
        lc.escalateLock();
      }
    }
    _accessedTransactions.add(tid);
    if (_uncommitDataMap.containsKey(varIndex)) {
      Data d = _uncommitDataMap.get(varIndex);
      d.setValue(value);
    } else {
      Data d = new Data(varIndex, value);
      _uncommitDataMap.put(varIndex, d);
    }
  }

  /**
   * Given a variable index and transaction, check whether the transaction can
   * write the variable, i.e. whether it can get the lock
   * 
   * @param tid
   * @param varIndex
   * @return true if can write, false if can't
   */
  public boolean isWritable(int tid, int varIndex) {
    return !hasConflict(tid, varIndex, Lock.Type.WRITE);
  }

  /**
   * Given a variable index return the list of transaction ids that have
   * conflicts, i.e. have the lock on this variable.
   * 
   * @param varIndex
   * @return list of transaction ids
   */
  public Set<Integer> getConflictTrans(int varIndex) {
    if (!_dataMap.containsKey(varIndex)) {
      return null;
    }
    Set<Integer> conflictSet = new HashSet<Integer>();
    if (_lockTable.containsKey(varIndex)) {
      List<Lock> lockList = _lockTable.get(varIndex);
      for (Lock lc : lockList) {
        conflictSet.add(lc.getTranId());
      }
    }
    return conflictSet;
  }

}

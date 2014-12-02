package edu.nyu.cs.adb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
/**
 * Database Manager (DM) is responsible for local site, collaborating 
 * with Transaction Manager (TM).
 * 
 * @author Jingxin Zhu
 * @author Wuping  Lei
 *
 */
public class DatabaseManager {

  // indicate whether site is up or down
  private boolean _siteStatus;

  private int _siteIndex;
  private TransactionManager _tm;

  // record the last time that the site fails
  private int _lastFailTime;

  // Map that stores all the data this site has, including multiversion
  private Map<Integer, List<Data>> _dataMap = new HashMap<Integer, List<Data>>();

  // Map that store all the dirty Data that written by some transactions before
  // commit
  private Map<Integer, Data> _uncommitDataMap = new HashMap<Integer, Data>();

  // lock table maintained by this site
  private Map<Integer, List<Lock>> _lockTable = new HashMap<Integer, List<Lock>>();

  // Set of all the transactions accessed in this site
  private Set<Integer> _accessedTransactions = new HashSet<Integer>();

  public DatabaseManager(int index, TransactionManager tm) {
    _siteStatus = true;
    _siteIndex = index;
    _tm = tm;
    _lastFailTime = -1;
  }

  /**
   * initialize the dataMap based on the site index.
   */
  public void init() {
    for (int i = 1; i <= 20; i++) {
      List<Data> dataList = new ArrayList<Data>();
      if (i % 2 == 0 || (1 + i % 10) == _siteIndex) {
        dataList.add(new Data(i, 10 * i));
        _dataMap.put(i, dataList);
      }
    }
  }

  /**
   * Get site index of database manager
   * @return
   */
  public int getIndex() {
    return _siteIndex;
  }

  /**
   * Get status of site
   * @return true if site is up, false if site is down.
   */
  public boolean getStatus() {
    return _siteStatus;
  }

  /**
   * Set site status.
   * @param status
   */
  public void setStatus(boolean status) {
    _siteStatus = status;
  }

  /* Set corresponding lock for given variable */
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

  /*
   * Get the corresponding lock for given variable and given transaction If the
   * transaction holds no lock return null
   */
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

  /*
   * Check whether there is conflict with the transaction on the variable i.e.
   * whether the transaction can get the lock it wants
   */
  private boolean hasConflict(int tid, int varIndex, Lock.Type type) {
    if (!_lockTable.containsKey(varIndex)) {
      return false;
    }
    List<Lock> lockList = _lockTable.get(varIndex);
    if (type == Lock.Type.READ) {
      for (Lock lc : lockList) {
        // The only conflict with read is some other transaction has a write
        // lock
        if (lc.getTranId() != tid && lc.getType() == Lock.Type.WRITE) {
          return true;
        }
      }
      return false;
    } else {
      for (Lock lc : lockList) {
        // As long as other transaction holds a lock, it will conflict with
        // write
        if (lc.getTranId() != tid) {
          return true;
        }
      }
      return false;
    }
  }

  /** 
   * Recover this site, for all the replicate variable, 
   * makes them unavailable 
   */
  public void recover() {
    _siteStatus = true;
    for (Integer varIndex : _dataMap.keySet()) {
      List<Data> dataList;
      if (varIndex % 2 == 0) {
        dataList = _dataMap.get(varIndex);
        Data d = dataList.get(dataList.size() - 1);
        // set the last commit variable to unavailable to read
        d.setAccess(false);
        // set the unavailable time for the variable which is the time it fails
        // When a particular version of variable is unavailable, it will never
        // become available, but we may have new version of variable
        d.setUnavailableTime(_lastFailTime);
      }
    }
  }

  /**
   * Set the site status to false, clear the lock table, accessedTransaction,
   * uncommitDataMap etc
   */
  public void fail() {
    _siteStatus = false;
    _lockTable.clear();
    _accessedTransactions.clear();
    _uncommitDataMap.clear();
    _lastFailTime = _tm.getCurrentTime();
  }

  /**
   * Return the data map this site has, used for dump all the variables
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
   *          variable index
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
   * When there is no read-only transaction runnning, clear all the old version
   * of all the variables on this site
   */
  public void clearAllVersions() {
    for (Integer varIndex : _dataMap.keySet()) {
      List<Data> dataList = _dataMap.get(varIndex);
      while (dataList.size() > 1) {
        dataList.remove(0);
      }
    }
  }

  /**
   * Commit the given transaction, write all the values in uncommitDataMap to
   * dataMap and release all the lock it holds
   * 
   * @param tid
   *          transaction id
   */
  public void commit(int tid) {
    List<Lock> lockList = null;

    // check wheter there is read-only transaction running
    boolean hasRO = _tm.hasRunningReadonly();
    for (Integer varIndex : _lockTable.keySet()) {
      lockList = _lockTable.get(varIndex);
      int size = lockList.size();
      for (int i = size - 1; i >= 0; i--) {
        Lock lc = lockList.get(i);
        if (lc.getTranId() == tid) {

          // If the lock type is write, means this transaction writes a variable
          // in uncommitDataMap
          if (lc.getType() == Lock.Type.WRITE) {
            if (_uncommitDataMap.containsKey(varIndex)) {
              List<Data> dataList = _dataMap.get(varIndex);
              Data d = _uncommitDataMap.get(varIndex);
              d.setCommitTime(_tm.getCurrentTime());

              // If no read-only transaction, replace the old version
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
    //remove this transaction from accessed list
    _accessedTransactions.remove(tid);
  }

  /**
   * Abort the given transaction, release all the locks it holds And erase the
   * data it has written
   * 
   * @param tid
   *          transaction id
   */
  public void abort(int tid) {
    List<Lock> lockList = null;
    for (Integer varIndex : _lockTable.keySet()) {
      lockList = _lockTable.get(varIndex);
      int size = lockList.size();
      for (int i = size - 1; i >= 0; i--) {
        if (lockList.get(i).getTranId() == tid) {
          if (lockList.get(i).getType() == Lock.Type.WRITE) {
            _uncommitDataMap.remove(varIndex);
          }
          lockList.remove(i);
          break;
        }
      }
    }
    //remove this transaction from accessed list
    _accessedTransactions.remove(tid);
  }

  /**
   * Given a variable index, read the data, if fails return null. Otherwise
   * return the variable update lock table and put the transaction into
   * accessedTransactions. Read only transaction would read the version it
   * needs. If a transaction has written this variable, reads it from
   * uncommitMapData
   * 
   * @param t
   *          transaction
   * @param varIndex
   *          variable index
   * @return data
   */
  public Data read(Transaction t, int varIndex) {
    if (!_dataMap.containsKey(varIndex)) {
      return null;
    }
    int tid = t.getTranId();

    // If the transaction is read-write, get the lock and see whether we have
    // conflict
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
          // If the variable is available return the variable
          if (d.getAccess()) {
            // If the transaction dosen't have a lock, set read lock
            if (lock == null) {
              setLock(tid, varIndex, Lock.Type.READ);
            }
            // Add this transaction to accessed transactions list
            _accessedTransactions.add(tid);
            return d;
          } else {
            return null;
          }
        } else {
          // if transaction has write lock, read it from uncommitDataMap
          _accessedTransactions.add(tid);
          return _uncommitDataMap.get(varIndex);
        }
      }
    } else {
      // For read-only transaction, get last commit version before it starts.
      List<Data> dataList = _dataMap.get(varIndex);
      Data d = null;
      int ttime = t.getTimestamp();
      for (Data dt : dataList) {
        if (dt.getCommitTime() <= ttime) {
          d = dt;
        } else {
          break;
        }
      }
      if (d == null) {
        return null;
      }
      // if available return the variable
      if (d.getAccess()) {
        return d;
      } else {
        // if not available, check whether it becomes unavailable (site fails)
        // after the read-only transaction begins, if so, the data is still
        // readable.
        if (d.getUnavailableTime() >= ttime) {
          return d;
        } else {
          // Otherwise, cannot be sure that the variable is the last commit
          // before read-only transaction begins
          return null;
        }
      }
    }
  }

  /**
   * Given a variable index, value to write and transaction, update the variable
   * and set the lock. But we update it in uncommitDataMap before commit, when
   * commit, writes all the values into dataMap
   * 
   * @param t
   *          transaction id
   * @param varIndex
   *          variable index
   * @param value
   *          the value to write into variable
   */
  public void write(Transaction t, int varIndex, int value) {
    int tid = t.getTranId();
    Lock lc = getLock(tid, varIndex);
    if (hasConflict(tid, varIndex, Lock.Type.WRITE)) {
      return;
    }
    if (lc == null) {
      // if transaction holds no lock, set write lock
      setLock(tid, varIndex, Lock.Type.WRITE);
    } else {
      // if transaction holds read lock before, escalate that lock
      if (lc.getType() == Lock.Type.READ) {
        lc.escalateLock();
      }
    }
    _accessedTransactions.add(tid);
    // put it into uncommitDataMap
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
   * write the variable, i.e. whether it can get the lock. If can, then get the
   * lock
   * 
   * @param tid
   *          transaction id
   * @param varIndex
   *          variable index
   * @return true if can write, false if can't
   */
  public boolean isWritable(int tid, int varIndex) {
    if (!hasConflict(tid, varIndex, Lock.Type.WRITE)) {
      setLock(tid, varIndex, Lock.Type.WRITE);
      _accessedTransactions.add(tid);
      return true;
    } else {
      return false;
    }
  }

  /**
   * Given a variable index return the list of transaction ids that have
   * conflicts, i.e. other transactions have the lock on this variable.
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

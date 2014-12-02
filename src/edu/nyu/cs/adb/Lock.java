package edu.nyu.cs.adb;

/**
 * This class abstracts the lock for variables, including
 * read lock and write lock.
 * @author Jingxin Zhu
 * @author Wuping  Lei
 *
 */
class Lock {
  
  static enum Type {
    READ, WRITE,
  };

  private int _transactionId;
  private Type _type;

  public Lock(int tid, Type type) {
    _transactionId = tid;
    _type = type;
  }

  /**
   * Get transaction id.
   * @return
   */
  public int getTranId() {
    return _transactionId;
  }

  /**
   * Get lock type.
   * @return
   */
  public Type getType() {
    return _type;
  }

  /*
   * escalate Lock, used when some transaction hold the read lock and want to
   * escalate to write lock for the same variable
   */
  public void escalateLock() {
    _type = Type.WRITE;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || !(o instanceof Lock))
      return false;
    Lock lc = (Lock) o;
    return _transactionId == lc._transactionId && _type == lc._type;
  }

  @Override
  public int hashCode() {
    int result = 17;
    result = 31 * result + Integer.valueOf(_transactionId).hashCode();
    result = 31 * result + _type.hashCode();
    return result;
  }
}

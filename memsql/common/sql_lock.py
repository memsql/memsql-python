from memsql.common.connection_pool import MySQLError
from memsql.common import errorcodes, sql_utility
import time
import uuid
from datetime import datetime

LOCK_TABLE = """\
CREATE TABLE IF NOT EXISTS %(name)s (
    id VARCHAR(255) PRIMARY KEY,
    lock_hash BINARY(32),
    owner VARCHAR(1024),
    last_contact DATETIME,
    expiry INT
)"""

class SQLLockManager(sql_utility.SQLUtility):
    def __init__(self, table_prefix="sqllock"):
        """ Initialize the SQLLockManager with the specified table prefix.
        """
        super(SQLLockManager, self).__init__()

        self.table_name = table_prefix.rstrip('_') + '_locks'
        self._define_table(self.table_name, LOCK_TABLE % { 'name': self.table_name })

    ###############################
    # Public Interface

    def acquire(self, lock_id, owner='', expiry=5 * 60, block=False, timeout=None, retry_interval=0.5):
        start = time.time()
        while 1:
            lock_ref = self._acquire_lock(lock_id, owner, expiry)
            if lock_ref is None and block:
                if timeout is not None and (time.time() - start) > timeout:
                    break
                time.sleep(retry_interval)
            else:
                break
        return lock_ref

    ###############################
    # Private Interface

    def _acquire_lock(self, lock_id, owner, expiry):
        try:
            with self._db_conn() as conn:
                conn.execute('''
                    DELETE FROM %(table_name)s
                    WHERE last_contact <= %%s - INTERVAL expiry SECOND
                ''' % { 'table_name': self.table_name }, datetime.utcnow())

                lock_hash = uuid.uuid1().hex.encode('utf8')
                conn.execute('''
                    INSERT INTO %s (id, lock_hash, owner, expiry, last_contact)
                    VALUES (%%s, %%s, %%s, %%s, %%s)
                ''' % self.table_name, lock_id, lock_hash, owner, expiry, datetime.utcnow())

                return SQLLock(lock_id=lock_id, lock_hash=lock_hash, owner=owner, manager=self)

        except MySQLError as err:
            (errno, msg) = err.args
            if errno == errorcodes.ER_DUP_ENTRY:
                return None
            else:
                raise

class SQLLock(object):
    def __init__(self, lock_id, lock_hash, owner, manager):
        self._manager = manager
        self._lock_id = lock_id
        self._lock_hash = lock_hash

        self.owner = owner

    ###############################
    # Public Interface

    def valid(self):
        with self._db_conn() as conn:
            row = conn.get('''
                SELECT
                    (lock_hash=%%s && last_contact > %%s - INTERVAL expiry SECOND) AS valid
                FROM %s WHERE id = %%s
            ''' % self._manager.table_name, self._lock_hash, datetime.utcnow(), self._lock_id)

        return bool(row is not None and row.valid)

    def ping(self):
        """ Notify the manager that this lock is still active. """
        with self._db_conn() as conn:
            affected_rows = conn.query('''
                UPDATE %s
                SET last_contact=%%s
                WHERE id = %%s AND lock_hash = %%s
            ''' % self._manager.table_name, datetime.utcnow(), self._lock_id, self._lock_hash)

        return bool(affected_rows == 1)

    def release(self):
        """ Release the lock. """
        if self.valid():
            with self._db_conn() as conn:
                affected_rows = conn.query('''
                    DELETE FROM %s
                    WHERE id = %%s AND lock_hash = %%s
                ''' % self._manager.table_name, self._lock_id, self._lock_hash)
            return bool(affected_rows == 1)
        else:
            return False

    ###############################
    # Context Management

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.release()

    ###############################
    # Public Interface

    def _db_conn(self):
        return self._manager._db_conn()

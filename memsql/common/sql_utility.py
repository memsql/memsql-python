from memsql.common.connection_pool import ConnectionPool
from memsql.common import exceptions

class SQLUtility(object):
    def __init__(self):
        self._pool = ConnectionPool()
        self._db_args = None
        self._tables = {}

    ###############################
    # Public Interface

    def connect(self, host='127.0.0.1', port=3306, user='root', password='', database=None):
        """ Connect to the database specified """

        if database is None:
            raise exceptions.RequiresDatabase()

        self._db_args = { 'host': host, 'port': port, 'user': user, 'password': password, 'database': database }
        with self._db_conn() as conn:
            conn.query('SELECT 1')
        return self

    def disconnect(self):
        self._pool.close()

    def setup(self):
        """ Initialize the required tables in the database """
        with self._db_conn() as conn:
            for table_defn in self._tables.values():
                conn.execute(table_defn)
        return self

    def destroy(self):
        """ Destroy the SQLStepQueue tables in the database """
        with self._db_conn() as conn:
            for table_name in self._tables:
                conn.execute('DROP TABLE IF EXISTS %s' % table_name)
        return self

    def ready(self):
        """ Returns True if the tables have been setup, False otherwise """
        with self._db_conn() as conn:
            tables = [row.t for row in conn.query('''
                SELECT table_name AS t FROM information_schema.tables
                WHERE table_schema=%s
            ''', self._db_args['database'])]
        return all([table_name in tables for table_name in self._tables])

    ###############################
    # Protected Interface

    def _define_table(self, table_name, table_definition):
        self._tables[table_name] = table_definition

    def _db_conn(self):
        if self._db_args is None:
            raise exceptions.NotConnected()
        return self._pool.connect(**self._db_args)

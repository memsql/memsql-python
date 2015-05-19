import time
import threading

from memsql.common import database

# Specify connection information for a MemSQL node
HOST = "127.0.0.1"
PORT = 3306
USER = "root"
PASSWORD = ""

# Specify which database and table to work with.
# Note: this database will be dropped at the end of this script
DATABASE = "test"
TABLE = "tbl"

# The number of workers to run
NUM_WORKERS = 20

# Run the workload for this many seconds
WORKLOAD_TIME = 10

# Batch size to use
BATCH_SIZE = 5000

# Pre-generate the workload query
QUERY_TEXT = "INSERT INTO %s VALUES %s" % (
    TABLE, ",".join(["()"] * BATCH_SIZE))

def get_connection(db=DATABASE):
    """ Returns a new connection to the database. """
    return database.connect(host=HOST, port=PORT, user=USER, password=PASSWORD, database=db)

class InsertWorker(threading.Thread):
    """ A simple thread which inserts empty rows in a loop. """

    def __init__(self, stopping):
        super(InsertWorker, self).__init__()
        self.stopping = stopping
        self.daemon = True
        self.exception = None

    def run(self):
        with get_connection() as conn:
            while not self.stopping.is_set():
                conn.execute(QUERY_TEXT)

def setup_test_db():
    """ Create a database and table for this benchmark to use. """

    with get_connection(db="information_schema") as conn:
        print('Creating database %s' % DATABASE)
        conn.query('CREATE DATABASE IF NOT EXISTS %s' % DATABASE)
        conn.query('USE %s' % DATABASE)

        print('Creating table %s' % TABLE)
        conn.query('CREATE TABLE IF NOT EXISTS tbl (id INT AUTO_INCREMENT PRIMARY KEY)')

def warmup():
    print('Warming up workload')
    with get_connection() as conn:
        conn.execute(QUERY_TEXT)

def run_benchmark():
    """ Run a set of InsertWorkers and record their performance. """

    stopping = threading.Event()
    workers = [ InsertWorker(stopping) for _ in range(NUM_WORKERS) ]

    print('Launching %d workers' % NUM_WORKERS)

    [ worker.start() for worker in workers ]
    time.sleep(WORKLOAD_TIME)

    print('Stopping workload')

    stopping.set()
    [ worker.join() for worker in workers ]

    with get_connection() as conn:
        count = conn.get("SELECT COUNT(*) AS count FROM %s" % TABLE).count

    print("%d rows inserted using %d workers" % (count, NUM_WORKERS))
    print("%.1f rows per second" % (count / float(WORKLOAD_TIME)))

def cleanup():
    """ Cleanup the database this benchmark is using. """

    with get_connection() as conn:
        conn.query('DROP DATABASE %s' % DATABASE)

if __name__ == '__main__':
    try:
        setup_test_db()
        warmup()
        run_benchmark()
    except KeyboardInterrupt:
        print("Interrupted... exiting...")
    finally:
        cleanup()

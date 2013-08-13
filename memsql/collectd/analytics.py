from datetime import datetime
import threading

CLASSIFIERS = [ "alpha", "beta", "gamma", "delta", "epsilon", "zeta" ]
ANALYTICS_COLUMNS = CLASSIFIERS + [ "created", "value", "classifier" ]

class AnalyticsRow(object):
    __slots__ = [
        'raw_value',
        'classifier',
        'created',
        'value'
    ]

    def __init__(self, created, raw_value, *args):
        self.created = created
        self.raw_value = raw_value

        classifier = []
        for value in args:
            value = self._parse_val(value)
            if value:
                classifier.append(value)
        [classifier.append("NULL") for i in range(len(CLASSIFIERS) - len(classifier))]
        self.classifier = tuple(classifier)

    def _parse_val(self, value):
        if value:
            return value.replace('.', '_')
        else:
            return None

    @property
    def iso_timestamp(self):
        return datetime.utcfromtimestamp(int(self.created)).strftime('%Y-%m-%d %H:%M:%S')

    def values(self):
        """ Returns the entire row as a tuple (classifier with created and value). """
        joined_classifier = '.'.join([x for x in self.classifier if x != 'NULL'])
        return self.classifier + (self.iso_timestamp, self.value, joined_classifier)

class AnalyticsCache(object):
    """ A thread safe cache for analytics rows.  Can efficiently flush
    all values in the cache using a multi-insert.
    """
    def __init__(self):
        self._lock = threading.RLock()
        self._pending = []
        self._previous_rows = {}

    def swap_row(self, new_row):
        """ Stores new_row in the cache and returns the previous row
        with the same hash or None.
        """
        classifier = new_row.classifier
        with self._lock:
            self._pending.append(new_row)
            previous_row, self._previous_rows[classifier] = self._previous_rows.get(classifier, None), new_row
        return previous_row

    def flush(self, conn):
        """ Generates a multi-insert statement and executes it against the analytics table. """

        # grab all the rows that need to be flushed
        with self._lock:
            flushing = [r for r in self._pending if hasattr(r, 'value')]
            self._pending = [r for r in self._pending if not hasattr(r, 'value')]

        value_template = ['(' + ','.join(['%s'] * len(ANALYTICS_COLUMNS)) + ')']
        columns = ','.join(ANALYTICS_COLUMNS)
        while len(flushing):
            batch, flushing = flushing[:50], flushing[50:]

            query_params = sum([row.values() for row in batch], ())
            values = ','.join(value_template * len(batch))

            conn.execute("INSERT INTO analytics (%s) VALUES %s" % (columns, values), *query_params)

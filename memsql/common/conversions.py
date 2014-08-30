import _mysql
from MySQLdb.constants import FIELD_TYPE
from MySQLdb.converters import conversions
from MySQLdb import times
import datetime

CONVERSIONS = conversions

def _bytes_to_utf8(b):
    return b.decode('utf-8')

CONVERSIONS[FIELD_TYPE.STRING].append((None, _bytes_to_utf8))
CONVERSIONS[FIELD_TYPE.VAR_STRING].append((None, _bytes_to_utf8))
CONVERSIONS[FIELD_TYPE.VARCHAR].append((None, _bytes_to_utf8))
CONVERSIONS[FIELD_TYPE.BLOB].append((None, _bytes_to_utf8))

def _escape_bytes(b, c):
    return _mysql.string_literal(b, c).decode('utf-8')

CONVERSIONS[bytes] = _escape_bytes

def _escape_string(s, d):
    return _mysql.string_literal(s.encode('utf-8'), d).decode('utf-8')

CONVERSIONS[str] = _escape_string

def _escape_datetime(dt, c):
    return times.DateTime2literal(dt, c).decode('utf-8')
def _escape_timedelta(dt, c):
    return times.DateTimeDelta2literal(dt, c).decode('utf-8')

CONVERSIONS[datetime.datetime] = _escape_datetime
CONVERSIONS[datetime.timedelta] = _escape_timedelta

from MySQLdb.constants import FIELD_TYPE, FLAG
from MySQLdb.converters import conversions, Bool2Str
from MySQLdb import times, _mysql
import datetime
import inspect

CONVERSIONS = conversions

def _bytes_to_utf8(b):
    return b.decode('utf-8')

CONVERSIONS[FIELD_TYPE.STRING] = ((FLAG.BINARY, bytes), (None, _bytes_to_utf8))
CONVERSIONS[FIELD_TYPE.VAR_STRING] = ((FLAG.BINARY, bytes), (None, _bytes_to_utf8))
CONVERSIONS[FIELD_TYPE.VARCHAR] = ((FLAG.BINARY, bytes), (None, _bytes_to_utf8))
CONVERSIONS[FIELD_TYPE.BLOB] = ((FLAG.BINARY, bytes), (None, _bytes_to_utf8))

def _escape_bytes(b, c):
    return _mysql.string_literal(b, c).decode('utf-8')

CONVERSIONS[bytes] = _escape_bytes

def _escape_string(s, d):
    return _mysql.string_literal(s.encode('utf-8')).decode('utf-8')

CONVERSIONS[str] = _escape_string

def _escape_datetime(dt, c):
    return times.DateTime2literal(dt, c).decode('utf-8')
def _escape_timedelta(dt, c):
    return times.DateTimeDelta2literal(dt, c).decode('utf-8')

CONVERSIONS[datetime.datetime] = _escape_datetime
CONVERSIONS[datetime.timedelta] = _escape_timedelta

def _escape_bool(b, d):
    return Bool2Str(b, d).decode('utf-8')
CONVERSIONS[bool] = _escape_bool

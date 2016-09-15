FIELD_TYPE_DICT = {
 0: 'DECIMAL',
 1: 'TINY',
 2: 'SHORT',
 3: 'LONG',
 4: 'FLOAT',
 5: 'DOUBLE',
 6: 'NULL',
 7: 'TIMESTAMP',
 8: 'LONGLONG',
 9: 'INT24',
 10: 'DATE',
 11: 'TIME',
 12: 'DATETIME',
 13: 'YEAR',
 14: 'NEWDATE',
 15: 'VARCHAR',
 16: 'BIT',
 246: 'NEWDECIMAL',
 247: 'INTERVAL',
 248: 'SET',
 249: 'TINY_BLOB',
 250: 'MEDIUM_BLOB',
 251: 'LONG_BLOB',
 252: 'BLOB',
 253: 'VAR_STRING',
 254: 'STRING',
 255: 'GEOMETRY'
}

N_A_TYPE = 'N/A'


def timedelta_total_seconds(td):
    """ Needed for python 2.6 compat """
    return (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10. ** 6) / 10. ** 6


def get_field_type_by_code(id):
    return FIELD_TYPE_DICT.get(id, N_A_TYPE)


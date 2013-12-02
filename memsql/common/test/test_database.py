import pytest
from memsql.common import database, exceptions

memsql_required = pytest.mark.skipif(
    "os.environ.get('TRAVIS') == 'true'",
    reason="requires MemSQL connection"
)


@memsql_required
def test_parameter_substitution(test_db_args, test_db_database):
    with database.connect(**test_db_args) as conn:
        conn.execute('CREATE DATABASE IF NOT EXISTS %s' % test_db_database)
        conn.execute('USE %s' % test_db_database)
        conn.execute('CREATE TABLE foo (a int primary key)')

        conn.query('INSERT INTO foo VALUES (%s)', 3)
        conn.query('INSERT INTO foo VALUES (%(val)s)', {'val' : 4})

        conn.query('INSERT INTO foo VALUES ("3%s")')
        conn.query('INSERT INTO foo VALUES ("3%(foo)s")')
        conn.query('INSERT INTO foo VALUES ("3%s"), ("3%(foo)s")')

        with pytest.raises(exceptions.FormatException):
            conn.query('INSERT INTO foo VALUES (%s), %(foo)s' % 3)
        with pytest.raises(exceptions.FormatException):
            conn.query('INSERT INTO foo VALUES (%s), %(foo)s' % {'foo' : 3})

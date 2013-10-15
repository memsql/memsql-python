import pytest
from memsql.common import database, exceptions
from memsql.common.sql_utility import SQLUtility

@pytest.fixture(scope="module")
def bootstrap(request, test_db_args, test_db_database):
    with database.connect(**test_db_args) as conn:
        conn.execute('CREATE DATABASE IF NOT EXISTS %s' % test_db_database)

@pytest.fixture()
def _db_args(test_db_args, test_db_database):
    test_db_args['database'] = test_db_database
    return test_db_args

def test_ensure_connected():
    u = SQLUtility()

    with pytest.raises(exceptions.NotConnected):
        u.setup()
    with pytest.raises(exceptions.NotConnected):
        u.destroy()
    with pytest.raises(exceptions.NotConnected):
        u.ready()

def test_connect(test_db_args, _db_args):
    u = SQLUtility()

    with pytest.raises(exceptions.RequiresDatabase):
        u.connect(test_db_args)

    with pytest.raises(exceptions.NotConnected):
        u.ready()

    u.connect(**_db_args)
    assert u.ready()

def test_setup(_db_args):
    u = SQLUtility().connect(**_db_args)
    assert u.ready()

    u._define_table('sql_utility', 'create table sql_utility (id bigint primary key)')
    assert not u.ready()

    u.setup()
    assert u.ready()

    with u._db_conn() as c:
        row = c.get('select table_name from information_schema.tables where table_name="sql_utility"')
        assert row is not None and row.table_name == 'sql_utility'

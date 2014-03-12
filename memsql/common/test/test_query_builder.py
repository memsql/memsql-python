from memsql.common import query_builder, database

def test_simple_expression():
    x = { 'a': 1, 'b': '2', 'c': 1223.4 }
    sql, params = query_builder.simple_expression(', ', **x)
    assert sql == '`a`=%(_QB_a)s, `b`=%(_QB_b)s, `c`=%(_QB_c)s'
    assert params == { '_QB_a': 1, '_QB_b': '2', '_QB_c': 1223.4 }
    assert database.escape_query(sql, params) == r"`a`=1, `b`='2', `c`=1223.4"

def test_update():
    x = { 'a': 1, 'b': '2', 'c': 1223.4 }
    sql, params = query_builder.update('foo', **x)
    assert sql == 'UPDATE `foo` SET `a`=%(_QB_a)s, `b`=%(_QB_b)s, `c`=%(_QB_c)s'
    assert params == { '_QB_a': 1, '_QB_b': '2', '_QB_c': 1223.4 }
    assert database.escape_query(sql, params) == r"UPDATE `foo` SET `a`=1, `b`='2', `c`=1223.4"

def test_multi_insert():
    rows = [{ 'a': 1, 'b': '2', 'c': 1223.4 }, { 'a': 2, 'b': '5', 'c': 1 }]
    sql, params = query_builder.multi_insert('foo', *rows)
    assert sql == 'INSERT INTO `foo` (`a`, `b`, `c`) VALUES (%(_QB_ROW_0)s), (%(_QB_ROW_1)s)'
    assert params == { '_QB_ROW_0': [1, '2', 1223.4], '_QB_ROW_1': [2, '5', 1] }
    assert database.escape_query(sql, params) == r"INSERT INTO `foo` (`a`, `b`, `c`) VALUES (1,'2',1223.4), (2,'5',1)"

def test_replace():
    rows = [{ 'a': 1, 'b': '2', 'c': 1223.4 }, { 'a': 2, 'b': '5', 'c': 1 }]
    sql, params = query_builder.multi_replace('foo', *rows)
    assert sql == 'REPLACE INTO `foo` (`a`, `b`, `c`) VALUES (%(_QB_ROW_0)s), (%(_QB_ROW_1)s)'
    assert params == { '_QB_ROW_0': [1, '2', 1223.4], '_QB_ROW_1': [2, '5', 1] }
    assert database.escape_query(sql, params) == r"REPLACE INTO `foo` (`a`, `b`, `c`) VALUES (1,'2',1223.4), (2,'5',1)"

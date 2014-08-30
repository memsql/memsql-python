def simple_expression(joiner=', ', **fields):
    """ Build a simple expression ready to be added onto another query.

    >>> simple_expression(joiner=' AND ', name='bob', role='admin')
    "`name`=%(_QB_name)s AND `name`=%(_QB_role)s", { '_QB_name': 'bob', '_QB_role': 'admin' }
    """
    expression, params = [], {}

    for field_name, value in sorted(fields.items(), key=lambda kv: kv[0]):
        key = '_QB_%s' % field_name
        expression.append('`%s`=%%(%s)s' % (field_name, key))
        params[key] = value

    return joiner.join(expression), params

def update(table_name, **fields):
    """ Build a update query.

    >>> update('foo_table', a=5, b=2)
    "UPDATE `foo_table` SET `a`=%(_QB_a)s, `b`=%(_QB_b)s", { '_QB_a': 5, '_QB_b': 2 }
    """
    prefix = "UPDATE `%s` SET " % table_name
    sets, params = simple_expression(', ', **fields)
    return prefix + sets, params

def multi_insert(table_name, *rows):
    """ Build a multi-insert query.
        Each row in rows should be a dict of { column_name: column_value }

    >>> multi_insert('foo_table', { 'a': 5, 'b': 2 }, { 'a': 5, 'b': 2 })
    "INSERT INTO `foo_table` (`a`, `b`) VALUES (%(_QB_ROW_0)s), (%(_QB_ROW_1)s)"
    """
    return __multi_insert(table_name, rows)

def multi_replace(table_name, *rows):
    """ Build a multi-replace query.
        Each row in rows should be a dict of { column_name: column_value }

    >>> multi_replace('foo_table', { 'a': 5, 'b': 2 }, { 'a': 5, 'b': 2 })
    "REPLACE INTO `foo_table` (`a`, `b`) VALUES (%(_QB_ROW_0)s), (%(_QB_ROW_1)s)"
    """
    return __multi_insert(table_name, rows, replace=True)

def __multi_insert(table_name, rows, replace=False):
    cols = sorted(rows[0].keys())
    prefix = '%s INTO `%s` (%s) VALUES ' % (
        'REPLACE' if replace else 'INSERT',
        table_name,
        ', '.join(['`%s`' % col for col in cols])
    )
    sql, params = [], {}

    for i, row in enumerate(rows):
        key = '_QB_ROW_%d' % i
        params[key] = [ v for c, v in sorted(row.items(), key=lambda kv: cols.index(kv[0])) ]
        sql.append('(%%(%s)s)' % key)

    return prefix + ', '.join(sql), params

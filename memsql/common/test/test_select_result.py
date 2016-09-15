# -*- coding: utf-8 -*-
from memsql.common import database
import random
import pytest
import simplejson as json

try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict

FIELDS = [('l\\u203pez', 'VARCHAR'), ('ಠ_ಠ', 'VARCHAR'), ('cloud', 'VARCHAR'), ('moon', 'VARCHAR'), ('water', 'VARCHAR'),
          ('computer', 'VARCHAR'), ('school', 'VARCHAR'), ('network', 'VARCHAR'),
          ('hammer', 'VARCHAR'), ('walking', 'VARCHAR'), ('mediocre', 'VARCHAR'), ('literature', 'VARCHAR'),
          ('chair', 'VARCHAR'), ('two', 'VARCHAR'), ('window', 'VARCHAR'), ('cords', 'VARCHAR'), ('musical', 'VARCHAR'),
          ('zebra', 'VARCHAR'), ('xylophone', 'VARCHAR'), ('penguin', 'VARCHAR'), ('home', 'VARCHAR'),
          ('dog', 'VARCHAR'), ('final', 'VARCHAR'), ('ink', 'VARCHAR'), ('teacher', 'VARCHAR'), ('fun', 'VARCHAR'), ('website', 'VARCHAR'),
          ('banana', 'VARCHAR'), ('uncle', 'VARCHAR'), ('softly', 'VARCHAR'), ('mega', 'VARCHAR'), ('ten', 'VARCHAR'),
          ('awesome', 'VARCHAR'), ('attatch', 'VARCHAR'), ('blue', 'VARCHAR'), ('internet', 'VARCHAR'), ('bottle', 'VARCHAR'),
          ('tight', 'VARCHAR'), ('zone', 'VARCHAR'), ('tomato', 'VARCHAR'), ('prison', 'VARCHAR'), ('hydro', 'VARCHAR'),
          ('cleaning', 'VARCHAR'), ('telivision', 'VARCHAR'), ('send', 'VARCHAR'), ('frog', 'VARCHAR'), ('cup', 'VARCHAR'),
          ('book', 'VARCHAR'), ('zooming', 'VARCHAR'), ('falling', 'VARCHAR'), ('evily', 'VARCHAR'), ('gamer', 'VARCHAR'),
          ('lid', 'VARCHAR'), ('juice', 'VARCHAR'), ('moniter', 'VARCHAR'), ('captain', 'VARCHAR'), ('bonding', 'VARCHAR')]

def test_result_order():
    raw_data = [[random.randint(1, 2 ** 32) for _ in range(len(FIELDS))] for _ in range(256)]
    res = database.SelectResult(FIELDS, raw_data)

    for i, row in enumerate(res):
        _FIELDS = list(map(lambda a: a[0], FIELDS))
        reference = dict(zip(_FIELDS, raw_data[i]))
        ordered = OrderedDict(zip(_FIELDS, raw_data[i]))
        doppel = database.Row(FIELDS, raw_data[i])

        assert doppel == row
        assert row == reference
        assert row == ordered
        assert list(row.keys()) == _FIELDS
        assert list(row.values()) == raw_data[i]
        assert sorted(row) == sorted(_FIELDS)
        assert list(row.items()) == list(zip(_FIELDS, raw_data[i]))
        assert list(row.values()) == raw_data[i]
        assert list(row.keys()) == _FIELDS
        assert list(row.items()) == list(zip(_FIELDS, raw_data[i]))

        for f in _FIELDS:
            assert f in row
            assert f in row
            assert row[f] == reference[f]
            assert row['cloud'] == reference['cloud']
            assert row[f] == ordered[f]
            assert row['cloud'] == ordered['cloud']

        assert dict(row) == reference
        assert dict(row) == dict(ordered)

        with pytest.raises(KeyError):
            row['derp']

        with pytest.raises(AttributeError):
            row.derp

        with pytest.raises(NotImplementedError):
            row.pop()

        with pytest.raises(NotImplementedError):
            reversed(row)

        with pytest.raises(NotImplementedError):
            row.update({'a': 'b'})

        with pytest.raises(NotImplementedError):
            row.setdefault('foo', 'bar')

        with pytest.raises(NotImplementedError):
            row.fromkeys((1,))

        with pytest.raises(NotImplementedError):
            row.clear()

        with pytest.raises(NotImplementedError):
            del row['mega']

        reference['foo'] = 'bar'
        reference['cloud'] = 'blah'
        ordered['foo'] = 'bar'
        ordered['cloud'] = 'blah'
        row['foo'] = 'bar'
        row['cloud'] = 'blah'

        assert row == reference
        assert dict(row) == reference
        assert len(row) == len(reference)
        assert row == ordered
        assert dict(row) == dict(ordered)
        assert len(row) == len(ordered)

        assert row.get('cloud') == reference.get('cloud')
        assert row.get('cloud') == ordered.get('cloud')
        assert row.get('NOPE', 1) == reference.get('NOPE', 1)
        assert row.get('NOPE', 1) == ordered.get('NOPE', 1)

        assert json.dumps(row, sort_keys=True) == json.dumps(reference, sort_keys=True)
        assert json.dumps(row, sort_keys=True) == json.dumps(ordered, sort_keys=True)

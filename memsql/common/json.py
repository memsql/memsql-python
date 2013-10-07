import simplejson

def simplejson_datetime_serializer(obj):
    """
    Designed to be passed as the default kwarg in simplejson.dumps.  Serializes dates and datetimes to ISO strings.
    """
    if hasattr(obj, 'isoformat'):
        return obj.isoformat()
    else:
        raise TypeError('Object of type %s with value of %s is not JSON serializable' % (type(obj), repr(obj)))

def _set_defaults(kwargs):
    kwargs.setdefault('separators', (',', ':'))
    kwargs.setdefault('default', simplejson_datetime_serializer)
    return kwargs

def dumps(data, **kwargs):
    return simplejson.dumps(data, **_set_defaults(kwargs))

def loads(data, **kwargs):
    return simplejson.loads(data, **kwargs)

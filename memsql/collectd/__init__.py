import collectd
from datetime import datetime
from memsql.common.random_aggregator_pool import RandomAggregatorPool

# This is a data structure shared by every callback. We store all the
# configuration and globals in it.
MEMSQL_DATA = dict(
    host=None,
    port=None,
    user='root',
    password='',
    database='dashboard',
    typesdb={},
    previous_values={}
)

###################################
## Initialization/Config functions

def memsql_config(config, data):
    """ Handle configuring collectd-memsql. """
    for child in config.children:
        collectd.debug("Config settings %s" % child.key)
        key = child.key.lower()
        if key == 'host':
            data['host'] = child.values[0]
        elif key == 'port':
            data['port'] = int(child.values[0])
        elif key == 'user':
            data['user'] = child.values[0]
        elif key == 'password':
            data['password'] = child.values[0]
        elif key == 'database':
            data['database'] = child.values[0]
        elif key == 'typesdb':
            for v in child.values:
                memsql_parse_types_file(v, data)

def memsql_init(data):
    """ Handles initializing collectd-memsql. """

    # verify we have required params
    assert data['host'] is not None, 'MemSQL host is not defined'
    assert data['port'] is not None, 'MemSQL port is not defined'

    collectd.info('Initializing collectd-memsql with %s:%s' % (data['host'], data['port']))

    # initialize the aggregator pool
    data['pool'] = RandomAggregatorPool(
        host=data['host'],
        port=data['port'],
        user=data['user'],
        password=data['password'],
        database=data['database']
    )

def memsql_shutdown(data):
    """ Handles terminating collectd-memsql. """

    collectd.info('Terminating collectd-memsql')
    data['pool'].close()

#########################
## Write/utility functions

def memsql_parse_types_file(path, data):
    """ This function tries to parse a collectd compliant types.db file.
    Basically stolen from collectd-carbon.
    """
    collectd.debug('memsql_parse_types_file')
    types = data['typesdb']

    f = open(path, 'r')

    for line in f:
        fields = line.split()
        if len(fields) < 2:
            continue

        type_name = fields[0]

        if type_name[0] == '#':
            continue

        v = []
        for ds in fields[1:]:
            ds = ds.rstrip(',')
            ds_fields = ds.split(':')

            if len(ds_fields) != 4:
                collectd.info('memsql_writer: cannot parse data source %s on type %s' % ( ds, type_name ))
                continue

            v.append(ds_fields)

        types[type_name] = v

    f.close()

def memsql_write(collectd_sample, data=None):
    """ Write handler for collectd.
    This function is called per sample taken from every plugin.  It is
    parallelized among multiple threads by collectd.
    """
    types = data['typesdb']
    if collectd_sample.type not in types:
        collectd.info('memsql_writer: do not know how to handle type %s. do you have all your types.db files configured?' % collectd_sample.type)
        return

    value_types = types[collectd_sample.type]

    if len(value_types) != len(collectd_sample.values):
        collectd.info('memsql_writer: differing number of values for type %s' % collectd_sample.type)
        return

    for (value_type, value) in zip(value_types, collectd_sample.values):
        persist_value(value, value_type[0], value_type[1], collectd_sample, data)

def persist_value(new_value, data_source_name, data_source_type, collectd_sample, data):
    """ Persist a new value to the database.
    Handles converting COUNTER and DERIVE types.
    """
    rate = None

    # special case counter and derive since we only care about the
    # "difference" between their values (rather than their actual value)
    if data_source_type == 'COUNTER' or data_source_type == 'DERIVE':
        key = (data_source_name,
               data_source_type,
               collectd_sample.plugin,
               collectd_sample.plugin_instance,
               collectd_sample.type,
               collectd_sample.type_instance)

        # get the previous values if they exist
        old_value, old_time = data['previous_values'].get(key, [None, None])

        # save the new value in previous values
        data['previous_values'][key] = (new_value, collectd_sample.time)

        if old_value is None:
            # this was the first value, nothing to do
            return

        # don't let the time delta fall below 1
        time_delta = float(max(1, collectd_sample.time - old_time))

        # The following formula handles wrapping COUNTER data types
        # around since COUNTER's should never be negative.
        # Taken from: https://collectd.org/wiki/index.php/Data_source
        if data_source_type == 'COUNTER' and new_value < old_value:
            if old_value < 2 ** 32:
                rate = (2 ** 32 - old_value + new_value) / time_delta
            else:
                rate = (2 ** 64 - old_value + new_value) / time_delta
        else:
            rate = (new_value - old_value) / time_delta

    elif data_source_type == 'GAUGE' or data_source_name == 'ABSOLUTE':
        rate = new_value

    else:
        collectd.debug("MemSQL collectd. Undefined data source %s" % data_source_type)
        return

    query_params = [
        datetime.fromtimestamp(int(collectd_sample.time)).strftime('%Y-%m-%d %H:%M:%S'),
        collectd_sample.host,
        collectd_sample.plugin,
        collectd_sample.type,
        collectd_sample.plugin_instance,
        collectd_sample.type_instance,
        data_source_name,
        rate
    ]

    with data['pool'].connect() as conn:
        conn.execute("""
            INSERT INTO analytics
            (created, host, plugin, type, instance, category, value_name, value)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, *query_params)

######################
## Register callbacks

collectd.register_config(memsql_config, MEMSQL_DATA)
collectd.register_init(memsql_init, MEMSQL_DATA)
collectd.register_write(memsql_write, MEMSQL_DATA)
collectd.register_shutdown(memsql_shutdown, MEMSQL_DATA)

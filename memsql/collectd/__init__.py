try:
    import collectd
except:
    pass

from memsql.common.random_aggregator_pool import RandomAggregatorPool
from memsql.collectd.analytics import AnalyticsCache, AnalyticsRow
from memsql.collectd import cluster
from wraptor.decorators import throttle

# This is a data structure shared by every callback. We store all the
# configuration and globals in it.
class _AttrDict(dict):
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)

    def __setattr__(self, name, val):
        self[name] = val

MEMSQL_DATA = _AttrDict(
    typesdb={},
    values_cache=AnalyticsCache(),
    node=None
)

MEMSQL_DATA.config = _AttrDict(
    host=None,
    port=3306,
    user='root',
    password='',
    database='dashboard',
    memsqlnode=None
)

###################################
## Lifecycle functions

def memsql_config(config, data):
    """ Handle configuring collectd-memsql. """
    parsed_config = {}
    for child in config.children:
        key = child.key.lower()
        val = child.values[0]
        if isinstance(val, (str, unicode)) and val.lower() in 'yes,true,on'.split(','):
            val = True
        elif isinstance(val, (str, unicode)) and val.lower() in 'no,false,off'.split(','):
            val = False
        parsed_config[key] = val

    for config_key in data.config:
        if config_key in parsed_config:
            data.config[config_key] = parsed_config[config_key]

    if 'typesdb' in parsed_config:
        memsql_parse_types_file(parsed_config['typesdb'], data)

def memsql_init(data):
    """ Handles initializing collectd-memsql. """

    # verify we have required params
    assert len(data.typesdb) > 0, 'At least 1 TypesDB file path must be specified'
    assert data.config.host is not None, 'MemSQL host is not defined'
    assert data.config.port is not None, 'MemSQL port is not defined'

    collectd.info('Initializing collectd-memsql with %s:%s' % (data.config.host, data.config.port))

    # initialize the aggregator pool
    data.pool = RandomAggregatorPool(
        host=data.config.host,
        port=int(data.config.port),
        user=data.config.user,
        password=data.config.password,
        database=data.config.database
    )

def memsql_shutdown(data):
    """ Handles terminating collectd-memsql. """

    collectd.info('Terminating collectd-memsql')
    data.pool.close()

#########################
## Read/Write functions

def memsql_read(data):
    if data.node is not None:
        # send memsql status first
        memsql_status = collectd.Values(plugin="memsql", plugin_instance="status", type="gauge")
        for name, value in data.node.status():
            memsql_status.dispatch(type_instance=name, values=[value])

            if name in cluster.COUNTER_STATUS_VARIABLES:
                memsql_status.dispatch(type="counter", type_instance=name, values=[value])

    elif data.config.memsqlnode or data.config.memsqlnode is None:
        throttled_find_node(data)

def memsql_write(collectd_sample, data):
    """ Write handler for collectd.
    This function is called per sample taken from every plugin.  It is
    parallelized among multiple threads by collectd.
    """

    if data.node is not None:
        throttled_update_alias(data, collectd_sample)

    # get the value types for this sample
    types = data.typesdb
    if collectd_sample.type not in types:
        collectd.info('memsql_writer: do not know how to handle type %s. do you have all your types.db files configured?' % collectd_sample.type)
        return

    value_types = types[collectd_sample.type]

    if len(value_types) != len(collectd_sample.values):
        collectd.info('memsql_writer: differing number of values for type %s' % collectd_sample.type)
        return

    # for each value in this sample, insert it into the cache
    for (value_type, value) in zip(value_types, collectd_sample.values):
        cache_value(value, value_type[0], value_type[1], collectd_sample, data)

    # finally flush the cache
    throttled_flush(data)

#########################
## Utility functions

@throttle(60)
def throttled_update_alias(data, collectd_sample):
    host = collectd_sample.host.replace('.', '_')
    data.node.update_alias(data.pool, host)

@throttle(60)
def throttled_find_node(data):
    data.node = cluster.find_node(data.pool)
    if data.node is not None:
        collectd.info('I am a MemSQL node: %s:%s' % (data.node.host, data.node.port))

def memsql_parse_types_file(path, data):
    """ This function tries to parse a collectd compliant types.db file.
    Basically stolen from collectd-carbon.
    """
    collectd.debug('memsql_parse_types_file')
    types = data.typesdb

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

@throttle(1)
def throttled_flush(data):
    with data.pool.connect() as conn:
        data.values_cache.flush(conn)

def cache_value(new_value, data_source_name, data_source_type, collectd_sample, data):
    """ Cache a new value into the analytics cache.  Handles converting
    COUNTER and DERIVE types.
    """
    new_row = AnalyticsRow(
        int(collectd_sample.time),
        new_value,
        collectd_sample.host,
        collectd_sample.plugin,
        collectd_sample.plugin_instance,
        collectd_sample.type,
        collectd_sample.type_instance,
        data_source_name
    )

    previous_row = data.values_cache.swap_row(new_row)

    # special case counter and derive since we only care about the
    # "difference" between their values (rather than their actual value)
    if (data_source_type == 'COUNTER' or data_source_type == 'DERIVE'):
        if previous_row is None:
            return

        old_value = previous_row.raw_value

        # don't let the time delta fall below 1
        time_delta = float(max(1, new_row.created - previous_row.created))

        # The following formula handles wrapping COUNTER data types
        # around since COUNTER's should never be negative.
        # Taken from: https://collectd.org/wiki/index.php/Data_source
        if data_source_type == 'COUNTER' and new_value < old_value:
            if old_value < 2 ** 32:
                new_row.value = (2 ** 32 - old_value + new_value) / time_delta
            else:
                new_row.value = (2 ** 64 - old_value + new_value) / time_delta

            if collectd_sample.plugin == 'cpu' and collectd_sample.type == 'cpu' and collectd_sample.type_instance == 'wait':
                # in virtualized environments, iowait sometimes wraps around and then back
                if new_row.value > (2 ** 31):
                    new_row.raw_value = previous_row.raw_value
                    new_row.value = previous_row.value
        else:
            # the default wrap-around formula
            new_row.value = (new_value - old_value) / time_delta

    elif data_source_type == 'GAUGE' or data_source_name == 'ABSOLUTE':
        new_row.value = new_value

    else:
        collectd.debug("MemSQL collectd. Undefined data source %s" % data_source_type)

######################
## Register callbacks

try:
    collectd.register_config(memsql_config, MEMSQL_DATA)
    collectd.register_init(memsql_init, MEMSQL_DATA)
    collectd.register_write(memsql_write, MEMSQL_DATA)
    collectd.register_shutdown(memsql_shutdown, MEMSQL_DATA)
    collectd.register_read(memsql_read, 1, MEMSQL_DATA)
except:
    # collectd not available
    pass

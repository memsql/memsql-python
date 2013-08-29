from netifaces import interfaces, ifaddresses, AF_INET
import socket
import hashlib
import re
from memsql.common.connection_pool import ConnectionPool

def find_node(connection_pool):
    # first get all the node ips and ports
    node_ip_to_port = {}
    node = None

    with connection_pool.connect() as conn:
        aggregators = conn.query('SHOW AGGREGATORS')
        leaves = conn.query('SHOW LEAVES')
        for node in (aggregators + leaves):
            if node.Host == '127.0.0.1':
                # this is the aggregator we are connecting to
                node['Host'] = conn.connection_info()[0]

            node_host = socket.gethostbyname(node.Host)
            if node_host == '127.0.0.1':
                # special case: if node_host is '127.0.0.1' then we are this node
                node_ip_to_port[node.Host] = node.Port
            else:
                node_ip_to_port[node_host] = node.Port

    # check to see if we are one of the nodes
    for ip in _addresses_iter():
        if ip in node_ip_to_port:
            # found our node
            node = Node(ip, node_ip_to_port[ip])

    # node not found
    return node

def _addresses_iter():
    for interface in interfaces():
        details = ifaddresses(interface)
        if AF_INET in details:
            for link in details[AF_INET]:
                yield link['addr']

class Node(object):
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self._node_id = hashlib.md5(self.host + str(self.port)).hexdigest()
        self._pool = ConnectionPool()

    def update_alias(self, connection_pool, alias):
        try:
            conn = connection_pool.connect_master()

            if conn:
                conn.execute('''
                    INSERT INTO node_alias (node_id, alias)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE alias=VALUES(alias)
                ''', self._node_id, alias)
        finally:
            if conn:
                conn.close()

    def connect(self):
        return self._pool.connect(
            host=self.host,
            port=self.port,
            user="dashboard",
            password="",
            database="information_schema")

    def status(self):
        with self.connect() as conn:
            rows = conn.query('SHOW STATUS EXTENDED')

        for row in rows:
            name = row.Variable_name
            try:
                value = self._parse_value(row.Value)
            except ValueError:
                continue
            yield (name, value)

    STATUS_CONSTS = re.compile(r"ms|MB|KB", re.I)

    def _parse_value(self, value):
        if self.STATUS_CONSTS.search(value):
            return float(value.split(" ")[0])
        return float(value)

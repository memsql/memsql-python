from memsql.common import database
from memsql.perf.network_tester import NetworkTester

master_agg = 'master.cs.memcompute.com'
test_node = 'leaf-1.cs.memcompute.com'
iterations = 100
payload_size = 1024 * 500

conn = database.connect(host=master_agg, user='root')
conn.execute('CREATE DATABASE IF NOT EXISTS performance')
conn.execute('SET GLOBAL max_allowed_packet=%d' % (1024 * 1024 * 10))

m = NetworkTester(payload_size=payload_size).connect(host=master_agg, user='root', database='performance')
if m.ready():
    m.destroy()
m.setup()

n = NetworkTester().connect(host=test_node, user='root', database='performance')

def pp(data, postfix, cb=lambda x: x):
    for k, v in data.items():
        print k, cb(v), postfix

print 'latancy'
pp(n.estimate_latency(), 'ms')

print '\nroundtrip'
pp(n.estimate_roundtrip(iterations), 'MB/s', lambda x: (x / 1024 / 1024))

print '\nupload'
pp(n.estimate_upload(iterations), 'MB/s', lambda x: (x / 1024 / 1024))

print '\ndownload'
pp(n.estimate_download(iterations), 'MB/s', lambda x: (x / 1024 / 1024))

conn.execute('DROP DATABASE performance')

from memsql.common import database
from memsql.perf.network_tester import NetworkTester

with database.connect(host='leaf-1.cs.memcompute.com', user='root') as c:
    c.execute('CREATE DATABASE IF NOT EXISTS performance')
    c.execute('SET GLOBAL max_allowed_packet=%d' % (1024 * 1024 * 10))

n = NetworkTester().connect(host='leaf-1.cs.memcompute.com', user='root', database='performance')

if n.ready():
    n.destroy()
if not n.ready():
    n.setup()

def pp(data, postfix):
    for k, v in data.items():
        print k, v, postfix

print 'latancy'
pp(n.estimate_latency(), 'ms')

print '\nthroughput'
pp(n.estimate_throughput(1024 * 1024, 100), 'B/s')

print '\nupload'
pp(n.estimate_upload(1024 * 1024, 100), 'B/s')

print '\ndownload'
pp(n.estimate_download(1024 * 1024, 100), 'B/s')

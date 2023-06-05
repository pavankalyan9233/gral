# An API to push graph data out of ArangoDB AFAP (as fast as possible)

Idea: ArangoDB is a graph database and thus stores graphs. Sometimes, it is
desirable to get a graph out of the database quickly to perform some special
analytics computation. The API described in this document aims to achieve
this goal as quickly as possible.

In many cases it is only necessary to extract the graph topology (vertex keys
and edges) without the attached data. Therefore it is desirable to not send
the whole graph data over the network, but only as little as is possible. To
this end, we want to move from variable length keys for vertices as quickly
as possible to some fixed length hash. Since we assume to extract the whole
graph anyway, we can detect hash collisions in the process of the vertex
upload as an aside, and so already use hashes rather than keys for sending
the edges.

One bottleneck we face is RocksDB itself. We can parallelize across shards
trivially, but then within a shard we must use multiple threads to read
from several RocksDB iterators at the same time

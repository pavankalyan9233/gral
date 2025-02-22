# A single process graph analytics system

**Idea**: The system runs (at first) in a single process. There is
an API to upload a (directed) graph. 

We assume that the whole graph can be stored in the RAM of the single
process.

We then develop multi-threaded algorithms to work with this graph in RAM.
Results can be queried from the engine, either by key or by hash.


## Identifiers for the vertices

We hash keys of vertices (arbitrary byte data) to a 64/128bit value (bit
size chosen to keep collisions few). Since we are in a single process,
we can detect collisions and distribute alternative hash values for one
of the collision keys. The alternative values are reported back to the
application. Therefore we can - if desired - upload the edges already by
just uploading the hashes of from- and to-vertices. We allow to upload
additional arbitrary byte data with each vertex and edge.

We assume that the whole graph can be stored in the RAM of the single
process. After upload, we can rearrange data so that we can use a simple
index (0 - N-1, where N is the number of vertices) to describe a vertex.
This way, we can use 32bit or 64bit values to describe vertices and can
use simple array indexing to access their data. Preparing an edge index
is now a simple sorting operation on the list of edges.


## Client-Ids

To not repeat an old mistake, we will make the complete API idempotent
and retryable in the following way:

Every request (which is not already idempotent) will get a `u64` as
"client-id" which is sent from the client along with the request. This
number has to be created by the client and must be unique (amongst the
requests from this client). If the server got the request and has
worked on it, it can now store the client-id alongside the data created
or modified in the server. If it then gets the same request (with the
same client-id) again, for example because of a retry after the
connection was broken, the server can reply with the same response as it
would have originally sent.


## API

Overview over the API calls:

All bodies are binary, all integers are network byte order.
A u16 is a 16-bit (two byte) unsigned integer.
A i32 is a 32-bit (four bytes) signed (twos complement) integer.
Where it says `varlen`, we mean the following:

 - 0x00 means special (for example following is hash instead of key, or
   in other contexts, might be "empty")
 - 0x01 - 0x7f means this value as length
 - if first byte has high bit set, then this is the first byte of a u32
   number, since this is the highest byte, we need to subtract 0x80000000
   in the end (or reset the high bit before conversion).

Error bodies generally look like this (typically for HTTP 400..599):

```
u32     error code, non-zero
varlen  length of error message (can be 0 for empty)
[u8]    error message in UTF-8
```

or in the JSON case:

```
{
    "error": true,
    "errorCode": 17,
    "errorMessage": "string"
}
```


### `GET /v1/version-binary`

This returns the version of the server and the protocol version.

Response code: 
  - 200 for OK

Response body:

```
u32        version of the server, where 0x10203 means version 1.2.3
u32        lowest version of API which is supported, currently always 1
u32        highest version of API which is supported, currently always 1
```

### `GET /v1/version`

This returns the version of the server and the protocol version.

Response code:
  - 200 for OK

Response body:

```
{
  "version": "1.2.3",
  "apiMinVersion": 1,
  "apiMaxVersion": 2
}
```

### `DELETE /v1/shutdown`

This asks the server to shut down gracefully.

Response code: 
  - 200 for OK

Response body:

```
u32        version of the server, where 0x10203 means version 1.2.3
```


### `POST /v1/create`

This will create a graph. The server can allocate memory since it has
already an upper bound for the number of vertices and number of edges.

Body:

```
u64       client-id for retry (to identify previously sent ops)
u64       maxNumberVertices: maximal number of vertices
u64       maxNumberEdges: maximal number of edges
u8        bitsForHash: 0 decide self, 64 or 128 to force the bit width
u8        storeKeys: 0 for no, 1 for yes, needs additional memory, but
          with 0 one cannot use keys for edge from and to values or for
          later result queries
```

Response code:
 - 201 CREATED
 - 400 BAD if anything is wrong, likely out of memory

Response body (for 201):

```
u64       client-id
u64       id of graph, for later reference
u8        bitsForHash: 64 or 128
```

or 400 BAD with error body (see above).


### `POST /v1/vertices`

Upload a batch of vertices. Their keys will be stored and hashed. Hash
collisions are detected and for each key which runs into a collision a
new exceptional hash is invented and reported back. This means that from
then on, hashes can be used to identify keys. Duplicate keys are not
allowed.

Note that if keys are not stored (see create operation), then we cannot
detect duplicate keys (but still hash collisions). In this case the
client has to guarantee unique keys. If a repeated key is sent, then
we would see it as a collision and return an exceptional hash for the
second time the key is sent.

```
u64      client-id
u64      id of graph
u32      number of vertices in this batch

and then repeated as often as the number of vertices says:

varlen   length of key (0 not allowed)
[u8]     key (as many bytes as varlen suggested), arbitrary byte
         sequence allowed
varlen   length of additional data (0 allowed for no data)
[u8]     additional data, arbitrary byte sequence allowed
...

If any key is empty, the whole batch is rejected. Vertices before the
one with the empty key might already have been inserted.

Note that the implementation is free to not detect duplicate keys! In
particular, in the case that keys are not stored at all, it might not
even be possible to detect duplicate keys. It is perfectly legal to only
detect this as a hash collision and assign a second invented exceptional
hash to the second occurrence of a key!

Response is 200 OK and with the following body:

```
u64      client-id
u32      number of exceptional hashes

and then repeated as often as the number of exceptional hashes says:

u32      index of key in body with an exceptional hash
u64/u128 exceptional hash (bit width as negotiated)
varlen   length of key (0 not allowed)
[u8]     bytes of key
...

or 400 BAD and error body as above. Duplicate keys are not a reason to
return 400.


### `POST /v1/sealVertices`

Seals the vertices of a graph. It is not allowed to send edges for a
graph before the vertices are sealed. It is not allowed to send further
vertices for a graph after vertices are sealed. It is allowed to send
`sealVertices` whilst some vertex inserts are still ongoing and the
`sealVertices` call will only return if all of them have been finished.
However, this is risky, since a vertex insert which is only worked on
after the `sealVertices` has finished, will no longer work.

Body:

```
u64     client-id
u64     id of graph
```

Response is 200 with this body:

```
u64     client-id
u64     id of graph
u64     number of vertices
```

If the graph id is not found (or is already sealed), we return 404 and 
an error body as described above.


### `POST /v1/edges`

Upload a batch of edges. Each edge must have a from and a to value,
which must point to already existing vertices. That is, one has to first
upload all vertices, then seal them, and then upload the edges. Edges
using non-existent keys are rejected.

Note that if keys of vertices are not stored on the server, we cannot
use keys for from or to. We have to use hashes in this case. If there
have been hash collisions, the client has to take care of exceptional
hashes. Normally, we do store keys of vertices and sending keys is
allowed. If keys are used but not stored, we automatically reject the
edge.

```
u64      client-id
u64      id of graph
u32      number of edges in this batch

and then repeated as often as the number of edges says:

varlen   length of from (0 means hash, that is u64/u128 following)
[u8]     key (as many bytes as varlen suggested, or hash if varlen was 0)
varlen   length of to (0 means hash, that is u64/u128 following)
[u8]     key (as many bytes as varlen suggested, or hash if varlen was 0)
varlen   length of additional data (0 allowed for no data)
[u8]     additional data
...

Response is 200 OK and with the following body:

```
u64      client-id
u32      number of rejected edges

and then repeated as often as the number of rejected edges says:

u32      index of rejected key in input body
u32      error code: 1 for "from not found", 2 for "to not found", 3 for
         "both from and to not found"
varlen   length of additional data (0 allowed for no data)
[u8]     additional data of the rejected edge
...

or 400 BAD and error body as above. Rejected edges are not a reason to
return 400.


### `POST /v1/sealEdges`

Seals the edges of a graph. It is not allowed to send queries or
computations for a graph before the vertices and edges are sealed. It
is not allowed to send further vertices for a graph after its vertices
are sealed. It is not allowed to send further edges for a graph after
its edges are sealed. It is allowed to send `sealEdges` whilst some edge
inserts are still ongoing and the `sealEdges` call will only return if
all of them have been finished. However, this is risky, since an edge
insert which is only worked on after the `sealEdges` has finished, will
no longer work.

Body:

```
u64     client-id
u64     id of graph
u32     index edges, if non-zero, an edge index will be computed for the graph
        If bit 1 (least significant bit) is set, edges will be indexed by from
        If bit 2 (second least sign. bit) is set, edges will be indexed by to
        Note that if this is not done, it will be done lazily, if a
        computation needs the edge index later.
```

Response is 200 with this body:

```
u64     client-id
u64     id of graph
u64     number of vertices
u64     number of edges
```

If the graph id is not found (or its edges are already sealed), we
return 404 and an error body as described above.


### `POST /v1/computeBinary`

Triggers a computation for a graph. This call actually only triggers the
computation and returns immediately.

Body:

```
u64     client-id
u64     id of graph
u32     algorithm number:
          1: weakly connected components
          2: strongly connected components
```

Response is 200 with this body:

```
u64     client-id
u64     id of graph
u64     computation-id
u32     algorithm number
```

The computation-id identifies this particular computation.

If the graph id is not found, we return 404 and an error body as
described above.

Note that computation IDs are not per graph but global!

Note that a computation keeps the graph alive, even if it was dropped
in the meantime!


### `POST /v1/compute`

Triggers a computation for a graph. This call actually only triggers the
computation and returns immediately.

Body:

```
{
    "clientId": "unique_string_for_this_computation",
    "graphId": "ID_OF_GRAPH",
    "algorithm": "ID_OF_ALGORITHM"
}
```

So far, we have the following algorithms:

```
  wcc: weakly connected components
  scc: strongly connected components
```

Response is 200 with this body:

```
{
    "clientId": "unique_string_for_this_computation",
    "graphId": "ID_OF_GRAPH",
    "algorithm": "ID_OF_ALGORITHM",
    "jobId": "ID OF COMPUTATION JOB"
}
```

The computation-id identifies this particular computation.

If the graph id is not found, we return 404 and an error body as
described above.

Note that computation IDs are not per graph but global!

Note that a computation keeps the graph alive, even if it was dropped
in the meantime!


### `POST /v1/weaklyConnectedComponents`

Computes the weakly connected components of a graph. This call actually
only triggers the computation and returns immediately.

Body:

```
u64     client-id
u64     id of graph
```

Response is 200 with this body:

```
u64     client-id
u64     id of graph
u64     computation-id
```

The computation-id identifies this particular computation.

If the graph id is not found, we return 404 and an error body as
described above.

Note that computation IDs are not per graph but global!

Note that a computation keeps the graph alive, even if it was dropped
in the meantime!


### `POST /v1/stronglyConnectedComponents`

Computes the strongly connected components of a graph. This call actually
only triggers the computation and returns immediately.

Body:

```
u64     client-id
u64     id of graph
```

Response is 200 with this body:

```
u64     client-id
u64     id of graph
u64     computation-id
```

The computation-id identifies this particular computation.

If the graph id is not found, we return 404 and an error body as
described above.

Note that computation IDs are not per graph but global!

Note that a computation keeps the graph alive, even if it was dropped
in the meantime!


### `PUT /v1/getProgress`

Retrieves a progress report for a computation. One needs the computation-id
of the computation.

Body:

```
u64     computation-id
```

If the computation is found (via its client-id), the response is 200
with this body:

```
u64     computation-id
u32     total progress (a number which indicates which progress number
        means completion, can be 1 for yes/no or 100 for percentages or
        any other positive number)
u32     progress so far (as number from 0 to "total progress")
varlen  length of result, will be 0 if there is not yet a result
[u8]    bytes of result as many as given in varlen
        In most cases, this will be a short list of u64, so the length
        will be a multiple of 8.
```

For example, the `weaklyConnectedComponents` algorithm could report the
result in a certain form, like this:

```
u32     number of connected components
```

If the computation is not found (via its client-id), the response is 404
with an error body as described above.


### `GET /v1/getProgress/{jobId}`

Retrieves a progress report for a computation. One needs the computation-id
of the computation.

If the computation is found (via its client-id), the response is 200
with this body:

```
{
    "jobId": "JOB_ID",
    "total": 100,
    "progress": 50,

```

`total`: total progress (a number which indicates which progress number
        means completion, can be 1 for yes/no or 100 for percentages or
        any other positive number)

`progress`: progress so far (as number from 0 to "total progress")

`result`: stringified response, only present if there is a result

For example, the `weaklyConnectedComponents` algorithm could report the
result in a certain form, like this:

```
{
    ...
    "result": "17"
}
```

If the computation is not found (via its client-id), the response is 404
with an JSON error body as described above.



### `PUT /v1/dropComputation`

Erase the results of a computation. One needs the client-id of the
computation. This can also abort a computation. But then it is no
longer possible to retrieve

Body:

```
u64     client-id
u64     computation-id
```

If the computation is found (via its computation-id), the response is 200
with this body:

```
u64     client-id
u64     computation-id
```

If the computation is not found (via its computation-id), the response is 404
with an error body as described above.

Note that this call also cancels a running computation (if supported by
the particular computation).


### `PUT /v1/getResultsByVertices`

This is used to get results from a computation in the case that there
is one result for every vertex of the graph. This is, for example, the
case for `weaklyConnectedComponents`. The algorithm returns for every
vertex a number which identifies the connected component it is in.
Two vertices will have the same numeric result if and only if they are
in the same weakly connected component.

Note that we do not use a client-id here, since this is in fact just a
GET with a body, so it is automatically idempotent.

One sends a body like this:

```
u64      computation-id
u32      number of vertices queried

and then for each vertex queried its key or hash:

varlen   length of key (or 0 for hash)
[u8]     key or hash, if varlen was 0
...
```

If all goes well, the computation is found and is finished and we have
results, we get this body back:

```
u64      computation-id
u32      number of results for vertices (including rejected)
u32      id of type of computation:
         1: weakly connected components
         2: strongly connected components
         ...

and then for each vertex:

varlen   length of key (or 0 if only hash given)
[u8]     bytes of key (or hash if only hash given)
varlen   length of data for this key
[u8]     result data (depends on algorithm)
         (For weakly/strongly connected components it is just one u64
          per vertex.)

if a vertex key or hash was rejected (not found), the length of the
result is 0.
```

For example, the weakly connected components might return a

```
varlen   length of key (or 0 if only hash known)
[u8]     bytes of key (or hash if only hash known)
varlen   8
u64      id of connected component
```

for each vertex or

```
varlen   length of key (or 0 if only hash known)
[u8]     bytes of key (or hash if only hash known)
varlen   0
```

if the vertex was rejected i.e. not found.

If the computation with the given id is not found at all, we return a
status code of `404 NOT FOUND`, and if the computation is found but not yet
finished, we return `503 SERVICE_UNAVAILABLE` and an error body as
described above.


### `PUT /v1/dropGraph`

Erase a graph, all pending computations will be aborted, all
computations will be erased and all data of the graph, too.

Body:

```
u64     client-id
u64     id of graph
```

If all is well, 200 is returned with this body:

```
u64     client-id
u64     id of graph [redundent, but we leave it in for now]
```

If the graph is not found, 404 is returned with an error body as
described above.


### `PUT /v1/dumpVertices`

Dump out the vertices of a graph in batches. Since this is essentially
a GET request with body, we do not use a client-id. The request is
automatically idempotent. It is allowed to return a smaller amount
of data than requested, both w.r.t. number of vertices and number of bytes.

Body:

```
u64    id of graph
u64    start index to dump
u32    batch size limit in number of vertices (will be adhered to)
u32    batch size limit in bytes (might overshoot by one vertex)
```

If all is well, 200 is returned with this body:

```
u64    id of graph [redundent, but we leave it in for now]
u64    start index of this batch
u64    final index of this batch plus 1
u64    total number of vertices in the graph
```

and then for each vertex:

```
varlen  length of key (or 0 if only hash known)
[u8]    bytes of key (or hash if only hash known)
varlen  length of additional data (0 allowed for no additional data)
[u8]    bytes of additional data
...
```


### `PUT /v1/dumpEdges`

Dump out the edges of a graph in batches. Since this is essentially
a GET request with body, we do not use a client-id. The request is
automatically idempotent. It is allowed to return a smaller amount
of data than requested, both w.r.t. number of edges and number of bytes.

Body:

```
u64    id of graph
u64    start index to dump
u32    batch size limit in number of edges (will be adhered to)
u32    batch size limit in bytes (might overshoot by one edge)
```

If all is well, 200 is returned with this body:

```
u64    id of graph [redundent, but we leave it in for now]
u64    start index of this batch
u64    final index of this batch plus 1
u64    total number of edges in the graph
```

and then for each vertex:

```
varlen  length of from (or 0 if only hash known)
[u8]    bytes of from (or hash if only hash known)
varlen  length of to (or 0 if only hash known)
[u8]    bytes of to (or hash if only hash known)
varlen  length of additional data (0 allowed for no additional data)
[u8]    bytes of additional data
...
```


### `POST /v1/getArangoDBGraph`

Reads a complete graph from ArangoDB.

Body is JSON:

```
{
  "clientId": "1231313123",    # decimal u64 number as string
  "endpoints": [ "http://localhost:8529" ],
  "useTls": false,
  "database": "_system",
  "vertexCollections": [
    {"name":"V", "fields":[]}      # will only fetch _id
  ],
  "edgeCollections": [
    {"name": "E", "fields":[]}     # will only fetch _from and _to
  ],
  "username": "root",    # need either username/password or jwt
  "password": "abc",
  "jwt": "asdqweuwqehuwqeqe",
  "threads": 10,
  "indexEdges": 3,
  "bitsForHash": 64,   (or 128)
  "storeKeys": true
}
```

This API will call ArangoDB and fetch graph data via the dump API
from all given vertex collections and edge collections. For vertex
collections, only the `_id` field will be fetched plus all additional
fields listed (they end up as JSON in the additional data). For edge
collections, only the `_from` and `_to` fields will be fetched plus all
additional fields listed (they end up as JSON in the additional data).

The result is 200 if all went well and an error code if things go wrong
(no connection, database or collections not found, or other probelems).
In case of a good result, the returned body is:

```
{
  "clientId": "12321312312",
  "graphNumber": 0,
  "numberOfVertices": 10,
  "numberOfEdges": 10,
  "bitsForHash": 64,
  "storeKeys": true,
}
```


## Sharding

Should we ever want to put a distributed system behind this API, we
have the following advantage: Since we compute hashes for the vertex
IDs, we can then use some top bits of the hashes for sharding.

This means we can do the collision detection by shard (and so we can
still do it!) and we can send edges to both the shard of its from and
its to entry. We end up with a distributed "smart graph" without the
client seeing anything of this.

Since all communication is over keys (or indeed hashes), we can always
compute the hash of the key and thus derive the shard.



# Memory usage

Variant with 64bit hashes:

Let V be the number of vertices and E be the number of edges.
Let S be the sum of the key sizes and T be the sum of the from and to sizes.

```
index_to_hash        V*8
hash_to_index        V*8 *2
exceptions           0
index_to_key         V*8 + S    (or 0 without store-keys)
vertex_data          0          (sum of bytes of vertex data)
vertex_data_offsets  V*8        (or 0 if no data sent)

edges                E*16
edge_data            0          (sum of bytes of vertex data)
edge_data_offsets    E*8        (or 0 if no data sent)

edge_index_by_from   V*8        (only if indexed)
edges_by_from        E*8        (only if indexed)
edge_index_by_to     V*8        (only if indexed)
edges_by_to          E*8        (only if indexed)

==> best case: V*(8+8+8) + E*16 = 24*V + 16*E 

  without vertex or edge data and keys and without edge index.

Edge index:

  additional V*(8+8) + E*(8+8) = 16*(V+E)

Connected components algorithmus:

  additional V*16 bytes during computation, V*8 after finishing.


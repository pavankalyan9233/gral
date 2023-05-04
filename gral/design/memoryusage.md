# Memory usage

Variant with 64bit hashes:

Let V be the number of vertices and E be the number of edges.
Let S be the sum of the key sizes and T be the sum of the from and to sizes.

```
index_to_hash        V*8
hash_to_index        V*8 *2
exceptions           0
index_to_key         V*8 + S    (or 0 without store-keys)
vertex_data          0
vertex_data_offsets  V*8

edges                E*24
edge_data            0
edge_index_by_from   V*8
edges_by_from        E*8
edge_index_by_to     V*8
edges_by_to          E*8

==> V*(8+16+8+8+8) + E*(24+8+8) = V*48 + E*40

Connected components algorithmus:

additionally V*16 bytes.

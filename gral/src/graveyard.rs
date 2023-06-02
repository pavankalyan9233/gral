pub fn dump_graph(&self) {
    println!("\nVertices:");
    println!("{:<32} {:<16} {}", "key", "hash", "data size");
    for i in 0..self.number_of_vertices() {
        let k = if self.store_keys {
            &self.index_to_key[i as usize]
        } else {
            "not stored".as_bytes()
        };
        let kkk: &str;
        let kk = str::from_utf8(k);
        if kk.is_err() {
            kkk = "non-UTF8-bytes";
        } else {
            kkk = kk.unwrap();
        }

        println!(
            "{:32} {:016x} {}",
            kkk,
            self.index_to_hash[i as usize].to_u64(),
            if self.vertex_data_offsets.is_empty() {
                0
            } else {
                self.vertex_data_offsets[i as usize + 1] - self.vertex_data_offsets[i as usize]
            }
        );
    }
    println!("\nEdges:");
    println!(
        "{:<15} {:<16} {:<15} {:16} {}",
        "from index", "from hash", "to index", "to hash", "data size"
    );
    for i in 0..(self.number_of_edges() as usize) {
        let size = if self.edge_data_offsets.is_empty() {
            0
        } else {
            self.edge_data_offsets[i + 1] - self.edge_data_offsets[i]
        };
        println!(
            "{:>15} {:016x} {:>15} {:016x} {}",
            self.edges[i].from.to_u64(),
            self.index_to_hash[self.edges[i].from.to_u64() as usize].to_u64(),
            self.edges[i].to.to_u64(),
            self.index_to_hash[self.edges[i].to.to_u64() as usize].to_u64(),
            size
        );
    }
}

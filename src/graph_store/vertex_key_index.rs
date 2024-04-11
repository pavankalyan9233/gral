use rand::Rng;
use std::mem::size_of;
use std::{collections::HashMap, fmt};
use xxhash_rust::xxh3::xxh3_64_with_seed;

#[derive(Eq, Hash, PartialEq, Clone, Copy, Ord, PartialOrd, Debug)]
pub struct VertexHash(u64);

impl VertexHash {
    pub fn new(x: u64) -> VertexHash {
        VertexHash(x)
    }
}

#[derive(Eq, PartialEq, Clone, Copy, Ord, PartialOrd, Debug)]
pub struct VertexIndex(u64);

impl VertexIndex {
    pub fn new(x: u64) -> VertexIndex {
        VertexIndex(x)
    }
    pub fn to_u64(self) -> u64 {
        self.0
    }
}

#[derive(PartialEq)]
pub struct VertexKeyIndex {
    // List of hashes by index:
    index_to_hash: Vec<VertexHash>,

    // key is the hash of the vertex, value is the index, high bit
    // indicates a collision
    hash_to_index: HashMap<VertexHash, VertexIndex>,

    // key is the key of the vertex, value is the exceptional hash
    exceptions: HashMap<Vec<u8>, VertexHash>,
}
impl fmt::Debug for VertexKeyIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VertexKeyIndex")
            .field("index_to_hash", &self.index_to_hash)
            .field("hash_to_index", &self.hash_to_index)
            .field("exceptions", &self.exceptions)
            .finish()
    }
}

impl VertexKeyIndex {
    pub fn new() -> Self {
        Self {
            index_to_hash: vec![],
            hash_to_index: HashMap::new(),
            exceptions: HashMap::new(),
        }
    }
    pub fn from(
        index_to_hash: Vec<VertexHash>,
        hash_to_index: HashMap<VertexHash, VertexIndex>,
        exceptions: HashMap<Vec<u8>, VertexHash>,
    ) -> Self {
        Self {
            index_to_hash,
            hash_to_index,
            exceptions,
        }
    }

    pub fn add(&mut self, hash: VertexHash) -> VertexIndex {
        // First detect a collision:
        let index = VertexIndex(self.index_to_hash.len() as u64);
        let mut actual = hash;
        if self.hash_to_index.contains_key(&hash) {
            // This is a collision, we create a random alternative
            // hash and report the collision:
            let mut rng = rand::thread_rng();
            loop {
                actual = VertexHash(rng.gen::<u64>());
                if !self.hash_to_index.contains_key(&actual) {
                    break;
                }
            }
            let oi = self.hash_to_index.get_mut(&hash).unwrap();
            *oi = VertexIndex(oi.0 | 0x800000000000000);
        }
        // Will succeed:
        self.index_to_hash.push(actual);
        self.hash_to_index.insert(actual, index);
        index
    }

    pub fn count(&self) -> usize {
        self.index_to_hash.len()
    }

    // TODO rename: pub fn get(key: &[u8]) -> VertexIndex;
    pub fn index_from_vertex_key(&self, k: &[u8]) -> Option<VertexIndex> {
        let hash: Option<VertexHash> = self.hash_from_vertex_key(k);
        match hash {
            None => None,
            Some(vh) => {
                let index = self.hash_to_index.get(&vh);
                index.copied()
            }
        }
    }

    pub fn memory_in_bytes(&self) -> usize {
        let size_hash = size_of::<VertexHash>();
        let size_index = size_of::<VertexIndex>();

        return self.count()
            * (
                // index_to_hash:
                size_hash
                    // hash_to_index:
		    + size_hash + size_index
            )
	    // Heuristics for the (hopefully few) exceptions:
            + self.exceptions.len() * (48 + size_hash);
    }

    fn hash_from_vertex_key(&self, k: &[u8]) -> Option<VertexHash> {
        let hash = VertexHash(xxh3_64_with_seed(k, 0xdeadbeefdeadbeef));
        let index = self.hash_to_index.get(&hash);
        match index {
            None => None,
            Some(index) => {
                if index.0 & 0x80000000_00000000 != 0 {
                    // collision!
                    let except = self.exceptions.get(k);
                    match except {
                        Some(h) => Some(*h),
                        None => Some(hash),
                    }
                } else {
                    Some(hash)
                }
            }
        }
    }
}


#[cfg(test)]
mod test {
    use super::*;

    #[test]
        fn generates_new_vertex_hash_for_already_existing_hash() {
            // let g_arc = Graph::new(true, vec![]);
            // let mut g = g_arc.write().unwrap();
            // g.insert_vertex(VertexHash::new(32), b"V/A".to_vec(), vec![]);

            // g.insert_vertex(VertexHash::new(32), b"V/B".to_vec(), vec![]);

            // assert_eq!(
            //     g.vertex_key_index,
            //     VertexKeyIndex::from(
            //         vec![hash_a, hash_b],
            //         HashMap::from([(hash_a, index_a), (hash_b, index_b)]),
            //         HashMap::new()
            //     )
            // );
            // assert_eq!(g.index_to_hash[0], VertexHash::new(32));
            // assert!(g.index_to_hash[1] != VertexHash::new(32));
        }
    }

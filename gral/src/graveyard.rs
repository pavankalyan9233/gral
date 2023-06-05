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


    let weakly_connected_components = warp::path!("v1" / "weaklyConnectedComponents")
        .and(warp::post())
        .and(with_graphs(graphs.clone()))
        .and(with_computations(computations.clone()))
        .and(warp::body::bytes())
        .and_then(api_weakly_connected_components);

        .or(weakly_connected_components)


pub struct WeaklyConnectedComponentsComputation {
    pub graph: Arc<RwLock<Graph>>,
    pub components: Option<Vec<u64>>,
    pub shall_stop: bool,
    pub number: u64,
}

impl Computation for WeaklyConnectedComponentsComputation {
    fn is_ready(&self) -> bool {
        self.components.is_some()
    }
    fn cancel(&mut self) {
        self.shall_stop = true;
    }
    fn algorithm_id(&self) -> u32 {
        return 1;
    }
    fn get_graph(&self) -> Arc<RwLock<Graph>> {
        return self.graph.clone();
    }
    fn dump_result(&self, out: &mut Vec<u8>) -> Result<(), String> {
        out.write_u8(8).unwrap();
        out.write_u64::<BigEndian>(self.number).unwrap();
        Ok(())
    }
    fn dump_vertex_results(
        &self,
        comp_id: u64,
        kohs: &Vec<KeyOrHash>,
        out: &mut Vec<u8>,
    ) -> Result<(), Rejection> {
        let comps = self.components.as_ref();
        match comps {
            None => {
                return Err(warp::reject::custom(ComputationNotYetFinished { comp_id }));
            }
            Some(result) => {
                let g = self.graph.read().unwrap();
                for koh in kohs.iter() {
                    let index = g.index_from_key_or_hash(koh);
                    match index {
                        None => {
                            put_key_or_hash(out, koh);
                            out.write_u8(0).unwrap();
                        }
                        Some(i) => {
                            put_key_or_hash(out, koh);
                            out.write_u8(8).unwrap();
                            out.write_u64::<BigEndian>(result[i.to_u64() as usize])
                                .unwrap();
                        }
                    }
                }
                return Ok(());
            }
        }
    }
}

/// This function triggers the computation of the weakly connected components
async fn api_weakly_connected_components(
    graphs: Arc<Mutex<Graphs>>,
    computations: Arc<Mutex<Computations>>,
    bytes: Bytes,
) -> Result<Vec<u8>, Rejection> {
    // Handle wrong length:
    if bytes.len() != 12 {
        return Err(warp::reject::custom(WrongBodyLength {
            found: bytes.len(),
            expected: 12,
        }));
    }

    // Parse body and extract integers:
    // (Note that we have checked the buffer length and so these cannot
    // fail! Therefore unwrap() is OK here.)
    let mut reader = Cursor::new(bytes.to_vec());
    let client_id = reader.read_u64::<BigEndian>().unwrap();
    let graph_number = reader.read_u32::<BigEndian>().unwrap();

    let graph_arc = get_graph(&graphs, graph_number)?;

    {
        // Check graph:
        let graph = graph_arc.read().unwrap();
        check_graph(graph.deref(), graph_number, true)?;
    }

    let comp_arc = Arc::new(Mutex::new(WeaklyConnectedComponentsComputation {
        graph: graph_arc.clone(),
        components: None,
        shall_stop: false,
        number: 0,
    }));

    let mut rng = rand::thread_rng();
    let mut comp_id: u64;
    {
        let mut comps = computations.lock().unwrap();
        loop {
            comp_id = rng.gen::<u64>();
            if !comps.list.contains_key(&comp_id) {
                break;
            }
        }
        comps.list.insert(comp_id, comp_arc.clone());
    }
    let _join_handle = std::thread::spawn(move || {
        let graph = graph_arc.read().unwrap();
        let (nr, components) = weakly_connected_components(&graph);
        println!("Found {} weakly connected components.", nr);
        let mut comp = comp_arc.lock().unwrap();
        comp.components = Some(components);
        comp.number = nr;
    });

    // Write response:
    let mut v = Vec::new();
    // TODO: handle errors!
    v.reserve(20);
    v.write_u64::<BigEndian>(client_id).unwrap();
    v.write_u32::<BigEndian>(graph_number).unwrap();
    v.write_u64::<BigEndian>(comp_id).unwrap();
    Ok(v)
}

    let version = warp::path!("v1" / "version")
        .and(warp::body::bytes())
        .map(|body: Bytes| {
            let s: String = format!("Input length: {}, version: {}", body.len(), VERSION);
            Response::builder()
                .header("X-Max-Header", "Hugo Honk")
                .body(s)
        });

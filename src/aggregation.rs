use crate::computations::{Component, ComponentsComputation, Computation};
use log::info;
use std::collections::HashMap;
use std::time::Instant;

pub fn aggregate_over_components(
    comp: &ComponentsComputation,
    attribute: String,
) -> Vec<Component> {
    let start = Instant::now();
    info!("Aggregating over components...");

    let mut result = vec![];
    result.reserve(1000000); // just heuristics

    let graph_arc = comp.get_graph();
    let graph = graph_arc.read().unwrap();

    // We should only be called if the computation is finished and has
    // produced a proper result!
    assert!(comp.components.is_some());
    assert!(comp.next_in_component.is_some());
    assert!(comp.number.is_some());
    let comps = comp.components.as_ref().unwrap();
    let next = &comp.next_in_component.as_ref().unwrap();
    assert_eq!(graph.index_to_hash.len(), comps.len());
    assert_eq!(comps.len(), next.len());
    assert_eq!(graph.vertex_json.len(), graph.index_to_hash.len());

    for i in 0..comps.len() {
        if comps[i] == i as u64 {
            // This is a representative
            let mut j = i as i64;
            let mut c = Component {
                key: i.to_string(),
                representative: match std::str::from_utf8(&graph.index_to_key[i]) {
                    Ok(s) => s.to_string(),
                    Err(_) => i.to_string(),
                },
                size: 1,
                aggregation: HashMap::new(),
            };
            let mut map = HashMap::<String, u64>::new();

            let extract_string = |pos: usize| -> Option<&str> {
                let v = &graph.vertex_json[pos];
                if v.is_string() {
                    return v.as_str();
                }
                if v.is_object() {
                    let s = &v[&attribute];
                    return s.as_str(); // Will return None if no string!
                }
                None
            };

            if let Some(st) = extract_string(i) {
                let s = st.to_string(); // Make a copy
                map.insert(s, 1);
            }
            let mut count: u64 = 1;
            while j != -1 {
                if let Some(s) = extract_string(j as usize) {
                    match map.get_mut(s) {
                        None => {
                            let st = s.to_string(); // Make a copy
                            map.insert(st, 1);
                        }
                        Some(c) => {
                            *c += 1;
                        }
                    };
                }
                j = next[j as usize];
                count += 1;
            }
            c.size = count;
            c.aggregation = map;
            result.push(c);
            if result.len() % 100000 == 0 {
                info!(
                    "{:?} Have aggregated over {} components out of {}",
                    start.elapsed(),
                    result.len(),
                    comp.number.unwrap()
                );
            }
        }
    }
    info!("Aggregation took time {:?}.", start.elapsed());
    result
}

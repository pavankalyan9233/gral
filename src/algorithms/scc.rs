use crate::graph_store::graph::Graph;
use log::info;
use std::time::Instant;

// We use the terminology as in Knuth:
// https://www-cs-faculty.stanford.edu/~knuth/fasc12a+.pdf

/// Returns the number of strongly connected components and a vector
/// of as many numbers as there are vertices, which contains for each
/// index the id of the strongly connected component of the vertex.
/// The id is the smallest index of a vertex in the same strongly connected
/// component.
pub fn strongly_connected_components(g: &Graph) -> Result<(u64, Vec<u64>, Vec<i64>), String> {
    if !g.is_indexed_by_from() {
        return Err("The graph is missing the from-neighbour index which is required for the strongly connected components algorithm.".to_string());
    }

    let start = Instant::now();
    let nr_v = g.number_of_vertices();
    let lambda = u64::MAX; // Lambda in Knuth
    let sent = nr_v; // SENT in Knuth

    // Working data, all number of vertices sized:
    info!(
        "{:?} Computing strongly connected components, number of vertices: {}, number of edges: {}",
        start.elapsed(),
        nr_v,
        g.number_of_edges()
    );
    info!("{:?} Allocating data...", start.elapsed());
    let mut parent: Vec<u64> = vec![];
    let mut arc: Vec<u64> = vec![];
    let mut link: Vec<u64> = vec![];
    let mut rep: Vec<u64> = vec![];

    // T1
    // Initialize parent vector:
    parent.resize(nr_v as usize, lambda);
    arc.resize(nr_v as usize, lambda);
    link.resize(nr_v as usize, lambda);
    rep.resize(nr_v as usize + 1, lambda);
    rep[nr_v as usize] = 0; // exception to simplify conditions

    let mut w: u64 = sent;
    let mut p: u64 = 0;
    let mut sink: u64 = sent;
    let mut root: u64;
    let mut count: u64 = 0; // number of connected components
    info!("{:?} Starting depth first search...", start.elapsed());
    while w > 0 {
        w -= 1;
        if parent[w as usize] != lambda {
            continue; // Already done, next one
        }
        // Start exploring from w:
        let mut v = w;
        parent[v as usize] = sent; // root of a spanning tree
        root = v;
        let from_index = g.from_index.as_ref().unwrap();

        // Prepare exploration from v:
        'T3: loop {
            // This is the outer main loop for the depth first search. We
            // return to this place whenever we start exploring from a new
            // vertex v.
            let mut a = from_index.vertex_offset[v as usize];
            p += 1;
            rep[v as usize] = p;
            link[v as usize] = sent;

            'T4: loop {
                // This is the inner main loop for the depth first
                // search. We return to this place whenever we want to
                // move to a new edge going out of the current vertex.
                // When we get here, the variables v (current vertex)
                // and a (current arc) must be set correctly.

                // First the case of doing another arc from here:
                let u: u64; // the vertex we move to
                if a < from_index.vertex_offset[v as usize + 1] {
                    // T5
                    u = from_index.sorted_neighbours[a as usize].to_u64();
                    a += 1;
                    // T6
                    if parent[u as usize] == lambda {
                        // a new vertex, move there
                        parent[u as usize] = v; // u discovered from v
                        arc[v as usize] = a; // for backtracking
                        v = u;
                        continue 'T3;
                    }
                    // Is u our root and we are in the last component?
                    if root == u && p == nr_v {
                        while v != root {
                            link[v as usize] = sink;
                            sink = v;
                            v = parent[v as usize];
                        }
                        // u = sent;   // ineffective, since we break T3
                        // T8
                        while rep[sink as usize] >= rep[v as usize] {
                            rep[sink as usize] = sent + v;
                            sink = link[sink as usize];
                        }
                        rep[v as usize] = sent + v;
                        count += 1;
                        break 'T3;
                    }
                    if rep[u as usize] < rep[v as usize] {
                        rep[v as usize] = rep[u as usize];
                        link[v as usize] = lambda;
                    }
                    continue 'T4;
                }
                // T7, finish with v:
                u = parent[v as usize];
                if link[v as usize] == sent {
                    // T8, new connected component
                    while rep[sink as usize] >= rep[v as usize] {
                        rep[sink as usize] = sent + v;
                        sink = link[sink as usize];
                    }
                    rep[v as usize] = sent + v;
                    count += 1;
                    if count % 1000000 == 0 {
                        info!(
                            "{:?} Have found {} connected component(s)",
                            start.elapsed(),
                            count
                        );
                    }
                    // fall through to T9
                } else {
                    if rep[v as usize] < rep[u as usize] {
                        rep[u as usize] = rep[v as usize];
                        link[u as usize] = lambda;
                    }
                    link[v as usize] = sink;
                    sink = v;
                    // fall through to T9
                }
                // T9, tree done?
                if u == sent {
                    break 'T3;
                }
                // Backtrack:
                v = u;
                a = arc[v as usize];
            }
        }
    }
    rep.pop(); // remove unneeded 0
    info!("{:?} Translating result...", start.elapsed());
    // Translate rep array:

    for i in 0..nr_v {
        rep[i as usize] -= sent;
    }
    info!(
        "{:?} Finished. Found {} strongly connected component(s).",
        start.elapsed(),
        count
    );
    Ok((
        count,
        rep,
        vec![], /* FIXME: Provide component list later */
    ))
}

#[cfg(test)]
mod tests {
    use crate::graph_store::examples::make_cyclic_graph;

    use super::*;

    #[test]
    fn does_not_run_when_graph_has_no_from_neighbour_index() {
        let g = Graph::create(
            vec!["V/A".to_string()],
            vec![("V/A".to_string(), "V/A".to_string())],
        );

        assert!(strongly_connected_components(&g).is_err());
    }

    #[test]
    fn gives_empty_results_on_empty_graph() {
        let mut g = Graph::create(vec![], vec![]);
        g.index_edges(true, false);

        let (components_count, components, next_in_component) =
            strongly_connected_components(&g).unwrap();

        assert_eq!(components_count, 0);
        assert_eq!(components, Vec::<u64>::new());
        assert_eq!(next_in_component, Vec::<i64>::new())
    }

    #[test]
    fn in_unconnected_graph_each_vertex_is_its_own_component() {
        let mut g = Graph::create(
            vec!["V/A".to_string(), "V/B".to_string(), "V/C".to_string()],
            vec![],
        );
        g.index_edges(true, false);

        let (components_count, components, _next_in_component) =
            strongly_connected_components(&g).unwrap();

        assert_eq!(components_count, 3);
        assert_eq!(components, vec![0, 1, 2]);
    }

    #[test]
    fn strongly_connected_vertices_lie_in_one_component() {
        let mut g = Graph::create(
            vec!["V/A".to_string(), "V/B".to_string()],
            vec![
                ("V/A".to_string(), "V/B".to_string()),
                ("V/B".to_string(), "V/A".to_string()),
            ],
        );
        g.index_edges(true, false);

        let (components_count, components, _next_in_component) =
            strongly_connected_components(&g).unwrap();

        assert_eq!(components_count, 1);
        assert_eq!(components.len(), 2);
        assert_eq!(components[0], components[1]);
    }

    #[test]
    fn vertices_connected_by_one_directed_edge_are_not_strongly_connected() {
        let mut g = Graph::create(
            vec!["V/A".to_string(), "V/B".to_string()],
            vec![("V/A".to_string(), "V/B".to_string())],
        );
        g.index_edges(true, false);

        let (components_count, components, _next_in_component) =
            strongly_connected_components(&g).unwrap();

        assert_eq!(components_count, 2);
        assert_eq!(components, vec![0, 1]);
    }

    #[test]
    fn cyclic_graph_has_one_component() {
        let g = make_cyclic_graph(10);

        let (components_count, components, _next_in_component) =
            strongly_connected_components(&g).unwrap();

        assert_eq!(components_count, 1);
        assert_eq!(components.len(), 10);
        assert_eq!(components[1], components[0]);
        assert_eq!(components[2], components[0]);
        assert_eq!(components[3], components[0]);
        assert_eq!(components[4], components[0]);
        assert_eq!(components[5], components[0]);
        assert_eq!(components[6], components[0]);
        assert_eq!(components[7], components[0]);
        assert_eq!(components[8], components[0]);
        assert_eq!(components[9], components[0]);
    }
}

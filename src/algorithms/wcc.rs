use crate::graph_store::graph::Graph;
use log::info;
use std::time::Instant;

/// Returns the number of weakly connected components and a vector
/// of as many numbers as there are vertices, which contains for each
/// index the id of the weakly connected component of the vertex.
/// The id is the smallest index of a vertex in the same weakly connected
/// component.
pub fn weakly_connected_components(g: &Graph) -> Result<(u64, Vec<u64>, Vec<i64>), String> {
    let start = Instant::now();
    let nr_v = g.number_of_vertices();
    let nr_e = g.number_of_edges();
    info!(
        "{:?} Weakly connected components: Have graph with {} vertices and {} edges.",
        start.elapsed(),
        nr_v,
        nr_e
    );
    info!("{:?} Creating mini...", start.elapsed());
    let mut mini: Vec<u64> = Vec::with_capacity(nr_v as usize);
    for i in 0..nr_v {
        mini.push(i);
    }
    info!("{:?} Creating next...", start.elapsed());
    let mut next: Vec<i64> = Vec::with_capacity(nr_v as usize);
    for _ in 0..nr_v {
        next.push(-1);
    }

    let mut nr_components = nr_v;

    info!(
        "{:?} Computing weakly connected components...",
        start.elapsed()
    );
    for (counter, e) in (0_u64..).zip(g.edges.iter()) {
        if counter % 10000000 == 0 {
            info!(
                "{:?} Have currently {} connected components with {} of {} edges processed.",
                start.elapsed(),
                nr_components,
                counter,
                nr_e
            );
        }
        let a = e.from().to_u64();
        let b = e.to().to_u64();
        let mut c = mini[b as usize];
        let mut rep = mini[a as usize];
        if c == rep {
            continue;
        }
        if c < rep {
            (c, rep) = (rep, c);
        }
        // Now c = mini[b] and rep = mini[a] and rep < c
        let first = c;
        loop {
            mini[c as usize] = rep;
            let d = next[c as usize];
            if d == -1 {
                break;
            }
            c = d as u64;
        }
        let second = next[rep as usize]; // can be -1!
        next[rep as usize] = first as i64;
        next[c as usize] = second;
        nr_components -= 1;
        if nr_components == 1 {
            break;
        }
    }
    info!(
        "{:?} Finished, found {} weakly connected component(s).",
        start.elapsed(),
        nr_components
    );
    Ok((nr_components, mini, next))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gives_empty_results_on_empty_graph() {
        let g = Graph::create(vec![], vec![]);

        let (components_count, components, next_in_component) =
            weakly_connected_components(&g).unwrap();

        assert_eq!(components_count, 0);
        assert_eq!(components, Vec::<u64>::new());
        assert_eq!(next_in_component, Vec::<i64>::new())
    }

    #[test]
    fn in_unconnected_graph_each_vertex_is_its_own_component() {
        let g = Graph::create(
            vec!["V/A".to_string(), "V/B".to_string(), "V/C".to_string()],
            vec![],
        );

        let (components_count, components, next_in_component) =
            weakly_connected_components(&g).unwrap();

        assert_eq!(components_count, 3);
        assert_eq!(components, vec![0, 1, 2]);
        assert_eq!(next_in_component, vec![-1, -1, -1])
    }

    #[test]
    fn connected_vertices_lie_in_one_component_represented_by_smallest_index() {
        let g = Graph::create(
            vec!["V/A".to_string(), "V/B".to_string(), "V/C".to_string()],
            vec![
                ("V/A".to_string(), "V/B".to_string()),
                ("V/A".to_string(), "V/C".to_string()),
            ],
        );

        let (components_count, components, next_in_component) =
            weakly_connected_components(&g).unwrap();

        assert_eq!(components_count, 1);
        assert_eq!(components, vec![0, 0, 0]);
        assert_eq!(next_in_component, vec![2, -1, 1]);
    }

    #[test]
    fn edge_direction_is_irrelevant() {
        let g = Graph::create(
            vec!["V/A".to_string(), "V/B".to_string(), "V/C".to_string()],
            vec![
                ("V/B".to_string(), "V/A".to_string()),
                ("V/A".to_string(), "V/C".to_string()),
            ],
        );

        let (components_count, components, next_in_component) =
            weakly_connected_components(&g).unwrap();

        assert_eq!(components_count, 1);
        assert_eq!(components, vec![0, 0, 0]);
        assert_eq!(next_in_component, vec![2, -1, 1]);
    }

    #[test]
    fn finds_all_components() {
        let g = Graph::create(
            vec![
                "V/A".to_string(),
                "V/B".to_string(),
                "V/C".to_string(),
                "V/D".to_string(),
                "V/E".to_string(),
                "V/F".to_string(),
            ],
            vec![
                ("V/A".to_string(), "V/B".to_string()),
                ("V/A".to_string(), "V/C".to_string()),
                ("V/E".to_string(), "V/F".to_string()),
            ],
        );

        let (components_count, components, next_in_component) =
            weakly_connected_components(&g).unwrap();

        assert_eq!(components_count, 3);
        assert_eq!(components, vec![0, 0, 0, 3, 4, 4]);
        assert_eq!(next_in_component, vec![2, -1, 1, -1, 5, -1]);
    }
}

use crate::graph_store::graph::Graph;

pub struct EdgeVector(pub Vec<f64>);
impl EdgeVector {
    // source matrix transposed times self
    pub fn project_to_edge_sources(self, graph: &Graph) -> VertexVector {
        // TODO make sure that graph has no dangling edges
        // TODO make sure that vec has as many entries as edges in graph
        let mut result: Vec<f64> = vec![0.; graph.number_of_vertices() as usize];
        graph
            .edges
            .iter()
            .map(|e| e.from().to_usize())
            .enumerate()
            .for_each(|(edge_id, source)| result[source] += self.0[edge_id]);
        VertexVector(result)
    }
}
pub struct VertexVector(pub Vec<f64>);
impl VertexVector {
    // target matrix times self
    pub fn project_to_incoming_edges(self, graph: &Graph) -> EdgeVector {
        // TODO make sure that graph has no dangling edges
        // TODO make sure that vec has as many entries as vertices in graph
        EdgeVector(
            graph
                .edges
                .iter()
                .map(|e| e.to().to_usize())
                .map(|target| self.0[target])
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_ulps_eq;

    #[test]
    fn targe_times_vec_takes_ith_item_from_vec_of_ith_edge_target() {
        let graph = Graph::create(
            vec!["V/A".to_string(), "V/B".to_string(), "V/C".to_string()],
            vec![
                ("V/A".to_string(), "V/A".to_string()),
                ("V/A".to_string(), "V/B".to_string()),
                ("V/A".to_string(), "V/C".to_string()),
                ("V/C".to_string(), "V/A".to_string()),
            ],
        );

        let result = VertexVector(vec![1., 0.1, 1.5]).project_to_incoming_edges(&graph);

        assert_eq!(result.0.len() as u64, graph.number_of_edges());
        assert_ulps_eq!(result.0[0], 1.);
        assert_ulps_eq!(result.0[1], 0.1);
        assert_ulps_eq!(result.0[2], 1.5);
        assert_ulps_eq!(result.0[3], 1.);
    }

    #[test]
    fn source_transposed_times_vec_sums_up_all_vec_entries_of_ith_edge_source() {
        let graph = Graph::create(
            vec!["V/A".to_string(), "V/B".to_string(), "V/C".to_string()],
            vec![
                ("V/A".to_string(), "V/A".to_string()),
                ("V/A".to_string(), "V/B".to_string()),
                ("V/A".to_string(), "V/C".to_string()),
                ("V/C".to_string(), "V/A".to_string()),
            ],
        );

        let result = EdgeVector(vec![1., 0.1, 1.5, 10.3]).project_to_edge_sources(&graph);

        assert_eq!(result.0.len() as u64, graph.number_of_vertices());
        assert_ulps_eq!(result.0[0], 1. + 0.1 + 1.5);
        assert_ulps_eq!(result.0[1], 0.);
        assert_ulps_eq!(result.0[2], 10.3);
    }
}

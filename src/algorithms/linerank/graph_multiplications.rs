use crate::graph_store::graph::Graph;

#[derive(Clone, Debug)]
pub struct EdgeVector(pub Vec<f64>);
impl EdgeVector {
    pub fn apply_transposed_source_matrix(self, graph: &Graph) -> VertexVector {
        assert_eq!(self.0.len(), graph.number_of_edges() as usize);
        let mut result: Vec<f64> = vec![0.; graph.number_of_vertices() as usize];
        graph
            .edges
            .iter()
            .map(|e| e.from().to_usize())
            .enumerate()
            .for_each(|(edge_id, source)| result[source] += self.0[edge_id]);
        VertexVector(result)
    }
    pub fn apply_transposed_target_matrix(self, graph: &Graph) -> VertexVector {
        assert_eq!(self.0.len(), graph.number_of_edges() as usize);
        let mut result: Vec<f64> = vec![0.; graph.number_of_vertices() as usize];
        graph
            .edges
            .iter()
            .map(|e| e.to().to_usize())
            .enumerate()
            .for_each(|(edge_id, source)| result[source] += self.0[edge_id]);
        VertexVector(result)
    }
    pub fn invert_elementwise(self) -> Self {
        Self(
            self.0
                .into_iter()
                .map(|x| match x {
                    x if approx::ulps_eq!(x, 0.) => 0.,
                    x => 1. / x,
                })
                .collect(),
        )
    }
    pub fn normalize_with(self, normalization: &Self) -> Self {
        Self(
            self.0
                .into_iter()
                .zip(normalization.0.iter())
                .map(|(x, norm)| x * norm)
                .collect(),
        )
    }
}
#[derive(Debug)]
pub struct VertexVector(pub Vec<f64>);
impl VertexVector {
    pub fn apply_target_matrix(self, graph: &Graph) -> EdgeVector {
        assert_eq!(self.0.len(), graph.number_of_vertices() as usize);
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
impl std::ops::Add for VertexVector {
    type Output = Self;
    fn add(self, rhs: Self) -> Self::Output {
        Self(self.0.into_iter().zip(rhs.0).map(|(x, y)| x + y).collect())
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

        let result = VertexVector(vec![1., 0.1, 1.5]).apply_target_matrix(&graph);

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

        let result = EdgeVector(vec![1., 0.1, 1.5, 10.3]).apply_transposed_source_matrix(&graph);

        assert_eq!(result.0.len() as u64, graph.number_of_vertices());
        assert_ulps_eq!(result.0[0], 1. + 0.1 + 1.5);
        assert_ulps_eq!(result.0[1], 0.);
        assert_ulps_eq!(result.0[2], 10.3);
    }
}

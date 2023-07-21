use crate::computations::{Component, ComponentsComputation};
use crate::graphs::Graph;
use log::info;
use std::time::Instant;

pub fn aggregate_over_components(_graph: &Graph, _comp: &ComponentsComputation) -> Vec<Component> {
    let start = Instant::now();
    info!("Aggregating over components...");
    info!("Aggregation took time {:?}.", start.elapsed());
    vec![]
}

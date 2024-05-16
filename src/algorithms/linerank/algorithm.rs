use crate::graph_store::graph::Graph;

pub fn line_rank(
    g: &Graph,
    supersteps: u32,
    damping_factor: f64,
) -> Result<(Vec<f64>, u32), String> {
    Ok((vec![], 0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_ulps_eq;
}

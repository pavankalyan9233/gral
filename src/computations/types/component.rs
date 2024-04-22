use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Component {
    #[serde(rename = "_key")]
    pub key: String,
    pub representative: String,
    pub size: u64,
    pub aggregation: HashMap<String, u64>,
}

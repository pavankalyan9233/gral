use std::io::Result;
fn main() -> Result<()> {
    let mut prost_build = prost_build::Config::new();
    prost_build
        .type_attribute(".", "#[derive(serde::Serialize,serde::Deserialize)]")
        .type_attribute(".", "#[serde(rename_all = \"camelCase\")]")
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_protos(&["proto/graphanalyticsengine.proto"], &["proto/"])
        .unwrap();
    Ok(())
}

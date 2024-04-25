use prost_wkt_build::*;
use std::io::Result;
use std::path::PathBuf;

fn main() -> Result<()> {
    // Produce code for our own API:
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("proto");
    let proto_files = vec![root.join("graphanalyticsengine.proto")];

    // Tell cargo to recompile if any of these proto files are changed
    for proto_file in &proto_files {
        println!("cargo:rerun-if-changed={}", proto_file.display());
    }

    let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    let descriptor_path = out_dir.join("proto_descriptor.bin");

    let mut prost_build = prost_build::Config::new();
    prost_build
        .type_attribute(".", "#[derive(serde::Serialize,serde::Deserialize)]")
        .type_attribute(".", "#[serde(default)]")
        // Save descriptors to file
        .file_descriptor_set_path(&descriptor_path)
        // Generate prost structs
        .compile_protos(&proto_files, &[root])?;

    // And add serde serialization and deserialization:
    let descriptor_set = std::fs::read(descriptor_path)?;
    let descriptor = FileDescriptorSet::decode(&descriptor_set[..])?;
    prost_wkt_build::add_serde(out_dir, descriptor);

    // Produce code for authentication service:
    let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    tonic_build::configure()
        .file_descriptor_set_path(out_dir.join("definition_descriptor.bin"))
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile(&["proto/definition.proto"], &["proto"])
        .unwrap();

    // Produce byte representation of our python script for the executor:
    std::fs::copy(
        "src/python/assets/base_functions.py",
        out_dir.join("base_functions.py"),
    )
    .expect("Failed to copy Python script to output directory");

    println!("cargo:rerun-if-changed=src/python/snippets/base_functions.py");

    Ok(())
}

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

    let descriptor_path =
        PathBuf::from(std::env::var("OUT_DIR").unwrap()).join("proto_descriptor.bin");

    let mut prost_build = prost_build::Config::new();
    prost_build
        // Save descriptors to file
        .file_descriptor_set_path(&descriptor_path)
        // Override prost-types with pbjson-types
        .compile_well_known_types()
        .extern_path(".google.protobuf", "::pbjson_types")
        // Generate prost structs
        .compile_protos(&proto_files, &[root])?;

    let descriptor_set = std::fs::read(descriptor_path)?;
    pbjson_build::Builder::new()
        .register_descriptors(&descriptor_set)?
        .build(&[".arangodb.cloud.internal.graphanalytics.v1"])?;

    // Produce code for authentication service:
    let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    tonic_build::configure()
        .file_descriptor_set_path(out_dir.join("definition_descriptor.bin"))
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile(&["proto/definition.proto"], &["proto"])
        .unwrap();

    Ok(())
}

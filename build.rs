use std::io::Result;
fn main() -> Result<()> {
    prost_build::compile_protos(
        &[
            "src/proto/auth.proto",
            "src/proto/consensus.proto",
            "src/proto/client.proto",
            "src/proto/rpc.proto",
            "src/proto/checkpoint.proto",
        ],
        &["src/proto"],
    )?;
    Ok(())
}

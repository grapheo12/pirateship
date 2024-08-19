// Copyright (c) Shubham Mishra. All rights reserved.
// Licensed under the Apache 2.0 License.


use std::io::Result;
fn main() -> Result<()> {
    prost_build::compile_protos(
        &[
            "src/proto/auth.proto",
            "src/proto/consensus.proto",
            "src/proto/client.proto",
            "src/proto/rpc.proto",
            "src/proto/checkpoint.proto",
            "src/proto/execution.proto",
        ],
        &["src/proto"],
    )?;
    Ok(())
}

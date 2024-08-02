pub mod consensus {
    include!(concat!(env!("OUT_DIR"), "/proto.consensus.rs"));
}
pub mod client {
    include!(concat!(env!("OUT_DIR"), "/proto.client.rs"));
}
pub mod rpc {
    include!(concat!(env!("OUT_DIR"), "/proto.rpc.rs"));
}
pub mod checkpoint {
    include!(concat!(env!("OUT_DIR"), "/proto.checkpoint.rs"));
}
pub mod execution {
    include!(concat!(env!("OUT_DIR"), "/proto.execution.rs"));
}
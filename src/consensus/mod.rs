pub mod handler;
pub mod leader_rotation;

pub mod proto {
    pub mod consensus {
        include!(concat!(env!("OUT_DIR"), "/proto.consensus.rs"));
    }
    pub mod client {
        include!(concat!(env!("OUT_DIR"), "/proto.client.rs"));
    }
    pub mod rpc {
        include!(concat!(env!("OUT_DIR"), "/proto.rpc.rs"));
    }
}
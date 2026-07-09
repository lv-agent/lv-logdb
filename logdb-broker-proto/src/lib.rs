//! Protobuf + tonic codegen for logdb-broker (cr-037).

pub mod pb {
    tonic::include_proto!("logdbbroker");
}

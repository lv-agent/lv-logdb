pub mod pb {
    tonic::include_proto!("logdbd");
}
pub mod auth;
pub mod replication;
pub mod service;

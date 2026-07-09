fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Standalone schema — broker.proto does not import logdbd.proto (the broker
    // defines its own forwarded Record, decoupled from logdbd's format).
    tonic_build::compile_protos("proto/broker.proto")?;
    Ok(())
}

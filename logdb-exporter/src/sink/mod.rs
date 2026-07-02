use logdbd::pb::Record;

pub trait Sink: Send {
    fn push(&mut self, records: &[Record]) -> Result<(), String>;
    fn name(&self) -> &str;
}

#[cfg(feature = "clickhouse")]
pub mod clickhouse;
pub mod stdout;

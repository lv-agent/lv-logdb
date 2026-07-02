use logdbd_proto::pb::Record;

/// Extension methods for [`Record`].
pub trait RecordExt {
    /// Content as UTF-8 string, if valid.
    fn content_str(&self) -> Option<&str>;
}

impl RecordExt for Record {
    fn content_str(&self) -> Option<&str> {
        std::str::from_utf8(&self.content).ok()
    }
}

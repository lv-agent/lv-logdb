#![no_main]
use libfuzzer_sys::fuzz_target;
use logdb::storage::format::{SegmentHeader, SEGMENT_HEADER_SIZE};
fuzz_target!(|data: &[u8]| {
    if data.len() >= SEGMENT_HEADER_SIZE {
        let mut buf = [0u8; SEGMENT_HEADER_SIZE];
        buf.copy_from_slice(&data[..SEGMENT_HEADER_SIZE]);
        let _ = SegmentHeader::deserialize(&buf);
    }
});

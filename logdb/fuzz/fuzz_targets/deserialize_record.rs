#![no_main]
use libfuzzer_sys::fuzz_target;
use logdb::storage::format::deserialize_record;
fuzz_target!(|data: &[u8]| { let _ = deserialize_record(data); });

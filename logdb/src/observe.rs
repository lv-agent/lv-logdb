//! Internal observability shim.
//!
//! When the `tracing` feature is enabled, the macros here forward to the
//! [`tracing`](https://docs.rs/tracing) crate (structured, subscriber-driven
//! logging). When it is disabled, they expand to nothing — so logdb stays
//! zero-extra-dependency by default.
//!
//! The macros accept the same token stream as the corresponding `tracing`
//! macros (e.g. structured fields: `log_info!(segment = id, "rolled")`), so the
//! call sites need no `#[cfg]` of their own.

#[cfg(feature = "tracing")]
macro_rules! log_debug {
    ($($arg:tt)*) => {
        tracing::debug!($($arg)*)
    };
}
#[cfg(feature = "tracing")]
macro_rules! log_info {
    ($($arg:tt)*) => {
        tracing::info!($($arg)*)
    };
}
#[cfg(feature = "tracing")]
macro_rules! log_warn {
    ($($arg:tt)*) => {
        tracing::warn!($($arg)*)
    };
}

#[cfg(not(feature = "tracing"))]
macro_rules! log_debug {
    ($($arg:tt)*) => {};
}
#[cfg(not(feature = "tracing"))]
macro_rules! log_info {
    ($($arg:tt)*) => {};
}
#[cfg(not(feature = "tracing"))]
macro_rules! log_warn {
    ($($arg:tt)*) => {};
}

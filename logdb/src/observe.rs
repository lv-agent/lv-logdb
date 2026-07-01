//! Internal observability shim.
//!
//! When the `tracing` feature is enabled, the `log_*!` macros forward to the
//! [`tracing`](https://docs.rs/tracing) crate (structured, subscriber-driven
//! logging). When `metrics` is enabled, the `metric_*!` macros forward to the
//! [`metrics`](https://docs.rs/metrics) facade (counters / histograms / gauges
//! — install a recorder, e.g. a Prometheus exporter, in the host to collect).
//! When a feature is off, its macros expand to nothing — so logdb stays
//! zero-extra-dependency by default, and call sites need no `#[cfg]` of their
//! own.

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

// ── metrics facade (counters / histograms / gauges) ─────────────────────────
//
// Same no-op-when-off pattern as the tracing macros above. The `metrics` crate
// macros accept `(name, value[, "label" => "v", …])`; we pass tokens through
// verbatim so call sites stay cfg-free.

#[cfg(feature = "metrics")]
macro_rules! metric_counter {
    ($name:expr, $n:expr $(,)?) => {
        metrics::counter!($name).increment($n)
    };
}
#[cfg(feature = "metrics")]
macro_rules! metric_histogram {
    ($name:expr, $dur:expr $(,)?) => {
        metrics::histogram!($name).record($dur)
    };
}
#[cfg(feature = "metrics")]
macro_rules! metric_gauge {
    ($name:expr, $val:expr $(,)?) => {
        metrics::gauge!($name).set(($val) as f64)
    };
}

#[cfg(not(feature = "metrics"))]
macro_rules! metric_counter {
    ($($arg:tt)*) => {};
}
#[cfg(not(feature = "metrics"))]
macro_rules! metric_histogram {
    ($($arg:tt)*) => {};
}
#[cfg(not(feature = "metrics"))]
macro_rules! metric_gauge {
    ($($arg:tt)*) => {};
}

// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Metrics for the OTAP DebugProcessor node.

use otap_df_telemetry::instrument::{Counter, SignalCounter};
use otap_df_telemetry_macros::metric_set;

// Re-exported for hot-path call sites (`consumed.add(Signal::Logs, n)`).
pub use otap_df_telemetry::instrument::Signal;

/// Pdata-oriented metrics for the OTAP DebugProcessor
#[metric_set(name = "processor.debug.pdata")]
#[derive(Debug, Default, Clone)]
pub struct DebugPdataMetrics {
    /// Number of items consumed, split per signal.
    ///
    /// Exported per the configured signal-metric schema: granular
    /// (`consumed_log_records` / `consumed_metric_points` / `consumed_spans`)
    /// or agnostic (`consumed_items` with a `signal` data-point attribute).
    #[signal_metric(verb = "consumed")]
    pub consumed: SignalCounter<u64>,
    /// Number of messages (OTLP requests) consumed, split per signal.
    ///
    /// Exported per the configured signal-metric schema: granular
    /// (`consumed_log_messages` / `consumed_metric_messages` /
    /// `consumed_trace_messages`, unit `{msg}`) or agnostic (`consumed_messages`
    /// with a `signal` data-point attribute).
    #[signal_metric(verb = "consumed", per = "message")]
    pub consumed_messages: SignalCounter<u64>,
    /// Number of events (structured logs) consumed
    #[metric(unit = "{event}")]
    pub events_consumed: Counter<u64>,
    /// number of span links consumed
    #[metric(unit = "{link}")]
    pub span_links_consumed: Counter<u64>,
    /// number of span events (structured logs) consumed
    #[metric(unit = "{event}")]
    pub span_events_consumed: Counter<u64>,
    /// Number of metrics consumed
    #[metric(unit = "{metric}")]
    pub metric_signals_consumed: Counter<u64>,
}

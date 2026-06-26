// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Metric and Attribute descriptor types for metrics reflection.

use serde::{Deserialize, Serialize};

/// The type of instrument used to record the metric. Must be one of the following variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Instrument {
    /// A monotonic sum.
    Counter,
    /// A signed sum that can go up and down.
    UpDownCounter,
    /// A value that can arbitrarily go up and down, used for temperature or current memory usage
    Gauge,
    /// Distribution of recorded values, used for latencies or request sizes
    Histogram,
    /// Pre-aggregated min/max/sum/count summary.
    ///
    /// Internally tracked as an `Mmsc` instrument; the dispatcher exports the
    /// aggregated snapshot as a synthetic OTel histogram without bucket counts.
    Mmsc,
}

/// Aggregation temporality for sum-like instruments.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Temporality {
    /// Each snapshot represents a delta over the reporting interval.
    Delta,
    /// Each snapshot represents the cumulative value at the time of reporting.
    Cumulative,
}

/// Selects which well-known per-signal metric schema a signal-split instrument
/// (`#[signal_metric]` / `SignalCounter`) is exported as.
///
/// See `docs/telemetry/signal-metric-schemas.md`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SignalSchema {
    /// One OTLP metric per signal, distinguished by name (e.g.
    /// `consumed_log_records`, `consumed_metric_points`, `consumed_spans`).
    #[default]
    Granular,
    /// A single metric (e.g. `consumed_items`) whose data points are
    /// distinguished by a `signal` attribute (`logs` / `metrics` / `traces`).
    Agnostic,
}

/// Numeric representation used by a metric field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricValueType {
    /// Unsigned 64-bit integer.
    U64,
    /// 64-bit floating point.
    F64,
}

/// A static data-point attribute (key, value) attached to a metric field.
///
/// Used to express the "agnostic" signal-metric schema, where several fields
/// share one metric `name` but are distinguished by a data-point attribute
/// (e.g. `("signal", "logs")`). See the dual signal-metric schemas RFC.
pub type StaticAttribute = (&'static str, &'static str);

/// Metadata describing a single field inside a metrics struct.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct MetricsField {
    /// Canonical metric name (e.g., "bytes.rx"). Uniquely identifies the metric.
    pub name: &'static str,
    /// The unit in which the metric is measured matching
    /// [Unified Code for Units of Measure](https://unitsofmeasure.org/ucum.html).
    pub unit: &'static str,
    /// Short human readable description extracted from the doc comment of the field.
    pub brief: &'static str,
    /// The type of instrument used to record the metric.
    pub instrument: Instrument,
    /// Aggregation temporality (only meaningful for sum-like instruments).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temporality: Option<Temporality>,
    /// The numeric representation for the metric values.
    pub value_type: MetricValueType,
    /// Static data-point attributes attached to every value of this field.
    ///
    /// Empty for the common case. When multiple fields in a set share the same
    /// `name` but carry distinct attributes here, they export as a single
    /// metric with one attributed data point per field (the agnostic schema).
    #[serde(skip_serializing_if = "is_empty_attributes")]
    pub attributes: &'static [StaticAttribute],
}

#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_empty_attributes(attrs: &&'static [StaticAttribute]) -> bool {
    attrs.is_empty()
}

/// Descriptor for a multivariate metrics.
#[derive(Debug, Serialize)]
pub struct MetricsDescriptor {
    /// Human-friendly group name.
    pub name: &'static str,
    /// Ordered field metadata.
    pub metrics: &'static [MetricsField],
}

/// Supported attribute value kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AttributeValueType {
    /// String attribute value
    String,
    /// Integer attribute value
    Int,
    /// Double-precision floating-point attribute value
    Double,
    /// Boolean attribute value
    Boolean,
    /// Map attribute value (key-value pairs)
    Map,
}

/// Metadata describing a single attribute field.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct AttributeField {
    /// Attribute key (canonical, may contain dots instead of underscores).
    pub key: &'static str,
    /// Short description extracted from doc comments.
    pub brief: &'static str,
    /// Value kind.
    pub r#type: AttributeValueType,
}

/// Descriptor for an attribute set.
#[derive(Debug)]
pub struct AttributesDescriptor {
    /// Human-friendly group name.
    pub name: &'static str,
    /// Ordered attribute field metadata.
    pub fields: &'static [AttributeField],
}

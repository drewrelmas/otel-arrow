// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Implementation of the Core nodes (receiver, exporter, processor).

/// Receiver implementations for core nodes.
pub mod receivers;

pub use receivers::syslog_cef_receiver;

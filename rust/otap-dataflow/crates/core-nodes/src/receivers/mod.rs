// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Receiver implementations for core nodes.

/// Fake telemetry traffic generator receiver.
pub mod fake_data_generator;
pub mod internal_telemetry_receiver;
/// Receiver that ingests Syslog/CEF over TCP or UDP.
pub mod syslog_cef_receiver;
pub mod topic_receiver;
